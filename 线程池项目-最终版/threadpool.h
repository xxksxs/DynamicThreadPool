#ifndef FINAL_THREADPOOL_H
#define FINAL_THREADPOOL_H

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

/// 线程池的工作模式。
enum class PoolMode
{
    MODE_FIXED,  ///< 始终保持初始线程数量。
    MODE_CACHED, ///< 根据任务压力扩容，并回收空闲的额外线程。
};

/// 线程池的生命周期状态。
enum class ThreadPoolState
{
    Created,
    Running,
    Stopping,
    Stopped,
};

/// 在线程池未运行或正在关闭时提交任务。
class ThreadPoolStopped : public std::runtime_error
{
public:
    ThreadPoolStopped()
        : std::runtime_error("thread pool is not running")
    {}
};

/// 任务队列持续满载，提交等待超时。
class TaskQueueFull : public std::runtime_error
{
public:
    TaskQueueFull()
        : std::runtime_error("thread pool task queue is full")
    {}
};

/**
 * @brief 支持固定线程数和动态扩缩容的通用线程池。
 *
 * 配置接口必须在 start() 前调用。shutdown() 会停止接收新任务，
 * 等待已提交任务执行完成，并 join 所有工作线程。
 */
class ThreadPool
{
public:
    ThreadPool() = default;

    ~ThreadPool()
    {
        shutdown();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    /// 设置工作模式；线程池启动后调用无效。
    void setMode(PoolMode mode)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == ThreadPoolState::Created)
        {
            mode_ = mode;
        }
    }

    /// 设置任务队列容量；容量必须大于 0。
    void setTaskQueMaxThreshHold(std::size_t size)
    {
        if (size == 0)
        {
            throw std::invalid_argument("task queue capacity must be greater than zero");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == ThreadPoolState::Created)
        {
            taskQueueMaxThreshold_ = size;
        }
    }

    /// 设置 MODE_CACHED 模式允许的最大线程数。
    void setThreadSizeThreshHold(std::size_t size)
    {
        if (size == 0)
        {
            throw std::invalid_argument("thread count threshold must be greater than zero");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == ThreadPoolState::Created)
        {
            threadSizeThreshold_ = size;
        }
    }

    /// 设置额外线程的最大空闲时间。
    void setThreadIdleTimeout(std::chrono::milliseconds timeout)
    {
        if (timeout <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument("thread idle timeout must be positive");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == ThreadPoolState::Created)
        {
            threadIdleTimeout_ = timeout;
        }
    }

    /**
     * @brief 启动线程池。
     * @throws std::logic_error 线程池已启动或已经停止。
     */
    void start(std::size_t initialThreadCount = std::thread::hardware_concurrency())
    {
        std::vector<std::unique_ptr<Worker>> workersToJoin;
        std::unique_lock<std::mutex> lock(mutex_);

        if (state_ != ThreadPoolState::Created)
        {
            throw std::logic_error("thread pool can only be started once");
        }

        if (initialThreadCount == 0)
        {
            initialThreadCount = 1;
        }

        initialThreadCount_ = initialThreadCount;
        threadSizeThreshold_ = std::max(threadSizeThreshold_, initialThreadCount_);
        state_ = ThreadPoolState::Running;

        try
        {
            workers_.reserve(initialThreadCount_);
            for (std::size_t i = 0; i < initialThreadCount_; ++i)
            {
                createWorkerLocked(true);
            }
        }
        catch (...)
        {
            state_ = ThreadPoolState::Stopping;
            notEmpty_.notify_all();
            notFull_.notify_all();
            workersToJoin = std::move(workers_);
            lock.unlock();
            joinWorkers(workersToJoin);
            lock.lock();
            activeThreadCount_ = 0;
            idleThreadCount_ = 0;
            state_ = ThreadPoolState::Stopped;
            throw;
        }
    }

    /**
     * @brief 提交任意可调用对象及其参数。
     *
     * 队列满时最多等待 1 秒。提交失败不会在 submitTask() 中直接抛出，
     * 而是通过返回的 future 在 get() 时传播异常。
     */
    template<class Func, class... Args>
    auto submitTask(Func&& func, Args&&... args)
        -> std::future<std::invoke_result_t<std::decay_t<Func>, std::decay_t<Args>...>>
    {
        using ReturnType =
            std::invoke_result_t<std::decay_t<Func>, std::decay_t<Args>...>;

        reapExitedWorkers();

        auto invocation =
            [callable = std::decay_t<Func>(std::forward<Func>(func)),
             arguments = std::make_tuple(std::forward<Args>(args)...)]() mutable
            -> ReturnType
        {
            return std::apply(
                [&callable](auto&&... storedArgs) -> ReturnType
                {
                    return std::invoke(
                        callable,
                        std::forward<decltype(storedArgs)>(storedArgs)...);
                },
                std::move(arguments));
        };

        auto packagedTask =
            std::make_shared<std::packaged_task<ReturnType()>>(std::move(invocation));
        std::future<ReturnType> result = packagedTask->get_future();

        std::unique_lock<std::mutex> lock(mutex_);
        if (state_ != ThreadPoolState::Running)
        {
            return makeExceptionalFuture<ReturnType>(ThreadPoolStopped{});
        }

        const bool queueReady = notFull_.wait_for(
            lock,
            submitTimeout_,
            [this]
            {
                return state_ != ThreadPoolState::Running ||
                    taskQueue_.size() < taskQueueMaxThreshold_;
            });

        if (!queueReady)
        {
            return makeExceptionalFuture<ReturnType>(TaskQueueFull{});
        }

        if (state_ != ThreadPoolState::Running)
        {
            return makeExceptionalFuture<ReturnType>(ThreadPoolStopped{});
        }

        taskQueue_.emplace([packagedTask] { (*packagedTask)(); });

        if (mode_ == PoolMode::MODE_CACHED &&
            taskQueue_.size() > idleThreadCount_ &&
            activeThreadCount_ < threadSizeThreshold_)
        {
            try
            {
                createWorkerLocked(false);
            }
            catch (...)
            {
                // 扩容失败不丢弃任务，已有工作线程仍可继续处理队列。
            }
        }

        lock.unlock();
        notEmpty_.notify_one();
        return result;
    }

    /**
     * @brief 排空任务队列并关闭线程池。
     *
     * 该函数可重复调用。它必须由线程池工作线程之外的线程调用。
     */
    void shutdown()
    {
        std::lock_guard<std::mutex> shutdownLock(shutdownMutex_);
        std::vector<std::unique_ptr<Worker>> workersToJoin;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ == ThreadPoolState::Stopped)
            {
                return;
            }

            if (state_ == ThreadPoolState::Created)
            {
                state_ = ThreadPoolState::Stopped;
                return;
            }

            state_ = ThreadPoolState::Stopping;
            workersToJoin = std::move(workers_);
        }

        notEmpty_.notify_all();
        notFull_.notify_all();
        joinWorkers(workersToJoin);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            activeThreadCount_ = 0;
            idleThreadCount_ = 0;
            state_ = ThreadPoolState::Stopped;
        }
    }

    /// 返回当前仍在运行的工作线程数。
    std::size_t currentThreadCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return activeThreadCount_;
    }

    /// 返回当前空闲工作线程数。
    std::size_t idleThreadCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return idleThreadCount_;
    }

    /// 返回线程池当前生命周期状态。
    ThreadPoolState state() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_;
    }

private:
    using Task = std::function<void()>;

    struct Worker
    {
        Worker(std::size_t workerId, bool isCoreWorker)
            : id(workerId)
            , core(isCoreWorker)
        {}

        std::size_t id;
        bool core;
        bool exited{false};
        std::thread thread;
    };

    template<class ReturnType, class Exception>
    static std::future<ReturnType> makeExceptionalFuture(Exception&& exception)
    {
        std::promise<ReturnType> promise;
        std::future<ReturnType> future = promise.get_future();
        promise.set_exception(
            std::make_exception_ptr(std::forward<Exception>(exception)));
        return future;
    }

    /// 调用方必须持有 mutex_。
    void createWorkerLocked(bool core)
    {
        auto worker = std::make_unique<Worker>(nextWorkerId_++, core);
        Worker* workerPointer = worker.get();
        workers_.push_back(std::move(worker));

        try
        {
            workerPointer->thread =
                std::thread([this, workerPointer] { workerLoop(workerPointer); });
        }
        catch (...)
        {
            workers_.pop_back();
            throw;
        }

        ++activeThreadCount_;
        ++idleThreadCount_;
    }

    void workerLoop(Worker* worker)
    {
        for (;;)
        {
            Task task;
            {
                std::unique_lock<std::mutex> lock(mutex_);

                if (mode_ == PoolMode::MODE_CACHED && !worker->core)
                {
                    const bool awakened = notEmpty_.wait_for(
                        lock,
                        threadIdleTimeout_,
                        [this]
                        {
                            return state_ != ThreadPoolState::Running ||
                                !taskQueue_.empty();
                        });

                    if (!awakened)
                    {
                        if (state_ == ThreadPoolState::Running &&
                            taskQueue_.empty() &&
                            activeThreadCount_ > initialThreadCount_)
                        {
                            markWorkerExitedLocked(worker);
                            return;
                        }
                        continue;
                    }
                }
                else
                {
                    notEmpty_.wait(
                        lock,
                        [this]
                        {
                            return state_ != ThreadPoolState::Running ||
                                !taskQueue_.empty();
                        });
                }

                if (state_ == ThreadPoolState::Stopping && taskQueue_.empty())
                {
                    markWorkerExitedLocked(worker);
                    return;
                }

                if (taskQueue_.empty())
                {
                    continue;
                }

                task = std::move(taskQueue_.front());
                taskQueue_.pop();
                --idleThreadCount_;
                notFull_.notify_one();
            }

            task();

            {
                std::lock_guard<std::mutex> lock(mutex_);
                ++idleThreadCount_;
                if (state_ == ThreadPoolState::Stopping && taskQueue_.empty())
                {
                    notEmpty_.notify_all();
                }
            }
        }
    }

    /// 调用方必须持有 mutex_，且当前工作线程处于空闲状态。
    void markWorkerExitedLocked(Worker* worker)
    {
        worker->exited = true;
        --activeThreadCount_;
        --idleThreadCount_;
    }

    void reapExitedWorkers()
    {
        std::vector<std::unique_ptr<Worker>> exitedWorkers;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto iterator = workers_.begin();
            while (iterator != workers_.end())
            {
                if ((*iterator)->exited)
                {
                    exitedWorkers.push_back(std::move(*iterator));
                    iterator = workers_.erase(iterator);
                }
                else
                {
                    ++iterator;
                }
            }
        }
        joinWorkers(exitedWorkers);
    }

    static void joinWorkers(std::vector<std::unique_ptr<Worker>>& workers)
    {
        for (auto& worker : workers)
        {
            if (worker->thread.joinable())
            {
                worker->thread.join();
            }
        }
        workers.clear();
    }

private:
    mutable std::mutex mutex_;
    std::mutex shutdownMutex_;
    std::condition_variable notFull_;
    std::condition_variable notEmpty_;

    std::queue<Task> taskQueue_;
    std::vector<std::unique_ptr<Worker>> workers_;

    std::size_t initialThreadCount_{0};
    std::size_t threadSizeThreshold_{1024};
    std::size_t activeThreadCount_{0};
    std::size_t idleThreadCount_{0};
    std::size_t taskQueueMaxThreshold_{std::numeric_limits<std::size_t>::max()};
    std::size_t nextWorkerId_{0};

    PoolMode mode_{PoolMode::MODE_FIXED};
    ThreadPoolState state_{ThreadPoolState::Created};
    std::chrono::milliseconds threadIdleTimeout_{std::chrono::seconds(60)};
    const std::chrono::milliseconds submitTimeout_{std::chrono::seconds(1)};
};

#endif
