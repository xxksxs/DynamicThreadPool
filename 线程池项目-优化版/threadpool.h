#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <iostream>
#include <vector>
#include <queue>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <unordered_map>
#include <thread>
#include <future>

const int TASK_MAX_THRESHHOLD = 2;       // 默认任务队列容量。
const int THREAD_MAX_THRESHHOLD = 1024;  // 动态模式的默认线程数上限。
const int THREAD_MAX_IDLE_TIME = 60;     // 额外线程的最大空闲时间，单位：秒。

/// 线程池的工作模式。
enum class PoolMode
{
	MODE_FIXED,  ///< 固定线程数，运行期间不动态增减。
	MODE_CACHED, ///< 按任务压力动态扩容，并回收长时间空闲的额外线程。
};

/**
 * @brief 工作线程的轻量封装。
 *
 * 每个 Thread 保存工作函数和线程池内部使用的逻辑编号。
 */
class Thread
{
public:
	/// 工作线程入口函数类型，参数为线程的逻辑编号。
	using ThreadFunc = std::function<void(int)>;

	Thread(ThreadFunc func)
		: func_(func)
		, threadId_(generateId_++)
	{}
	~Thread() = default;

	/// 创建并启动底层线程。
	void start()
	{
		std::thread t(func_, threadId_);
		// 线程生命周期由线程池的退出协议管理，不通过 Thread 对象执行 join。
		t.detach();
	}

	/// 返回线程池内部使用的逻辑编号。
	int getId()const
	{
		return threadId_;
	}
private:
	ThreadFunc func_;       ///< 工作线程入口函数。
	static int generateId_; ///< 下一个可分配的逻辑线程编号。
	int threadId_;          ///< 当前线程的逻辑编号。
};

int Thread::generateId_ = 0;

/**
 * @brief 基于可调用对象和 std::future 的通用线程池。
 *
 * submitTask() 接收任意可调用对象及其参数，并返回对应的 future。
 * 线程池支持固定线程数和动态缓存两种模式；所有配置应在 start() 前完成。
 */
class ThreadPool
{
public:
	/// 初始化配置和计数器，此时尚未创建工作线程。
	ThreadPool()
		: initThreadSize_(0)
		, taskSize_(0)
		, idleThreadSize_(0)
		, curThreadSize_(0)
		, taskQueMaxThreshHold_(TASK_MAX_THRESHHOLD)
		, threadSizeThreshHold_(THREAD_MAX_THRESHHOLD)
		, poolMode_(PoolMode::MODE_FIXED)
		, isPoolRunning_(false)
	{}

	/// 通知工作线程退出，并等待全部线程资源回收。
	~ThreadPool()
	{
		isPoolRunning_ = false;

		std::unique_lock<std::mutex> lock(taskQueMtx_);
		// 唤醒正在等待任务的线程，使其检查关闭状态。
		notEmpty_.notify_all();
		exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
	}

	/// 设置工作模式；线程池启动后调用无效。
	void setMode(PoolMode mode)
	{
		if (checkRunningState())
			return;
		poolMode_ = mode;
	}

	/// 设置任务队列允许容纳的最大任务数；线程池启动后调用无效。
	void setTaskQueMaxThreshHold(int threshhold)
	{
		if (checkRunningState())
			return;
		taskQueMaxThreshHold_ = threshhold;
	}

	/// 设置 MODE_CACHED 模式下允许创建的最大线程数。
	void setThreadSizeThreshHold(int threshhold)
	{
		if (checkRunningState())
			return;
		if (poolMode_ == PoolMode::MODE_CACHED)
		{
			threadSizeThreshHold_ = threshhold;
		}
	}

	/**
	 * @brief 提交任意可调用对象及其参数。
	 * @return 与任务返回类型对应的 std::future。
	 *
	 * 参数通过完美转发绑定到 packaged_task。若任务队列持续满 1 秒，
	 * 返回一个已经就绪、值为返回类型默认值的 future。
	 */
	template<typename Func, typename... Args>
	auto submitTask(Func&& func, Args&&... args) -> std::future<decltype(func(args...))>
	{
		// packaged_task 将任务执行结果与 future 关联起来。
		using RType = decltype(func(args...));
		auto task = std::make_shared<std::packaged_task<RType()>>(
			std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
		std::future<RType> result = task->get_future();

		std::unique_lock<std::mutex> lock(taskQueMtx_);
		// 队列已满时最多等待 1 秒，避免提交线程永久阻塞。
		if (!notFull_.wait_for(lock, std::chrono::seconds(1),
			[&]()->bool { return taskQue_.size() < (size_t)taskQueMaxThreshHold_; }))
		{
			std::cerr << "task queue is full, submit task fail." << std::endl;
			// 构造一个立即完成的替代任务，使调用方仍能安全调用 get()。
			auto task = std::make_shared<std::packaged_task<RType()>>(
				[]()->RType { return RType(); });
			(*task)();
			return task->get_future();
		}

		// 将带返回值的 packaged_task 包装为统一的 void() 任务。
		taskQue_.emplace([task]() {(*task)();});
		taskSize_++;

		// 入队后唤醒等待任务的工作线程。
		notEmpty_.notify_all();

		// 动态模式下，当排队任务多于空闲线程时，按需创建额外线程。
		if (poolMode_ == PoolMode::MODE_CACHED
			&& taskSize_ > idleThreadSize_
			&& curThreadSize_ < threadSizeThreshHold_)
		{
			std::cout << ">>> create new thread..." << std::endl;

			auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			int threadId = ptr->getId();
			threads_.emplace(threadId, std::move(ptr));
			threads_[threadId]->start();
			curThreadSize_++;
			idleThreadSize_++;
		}

		return result;
	}

	/// 创建初始工作线程并启动线程池。
	void start(int initThreadSize = std::thread::hardware_concurrency())
	{
		isPoolRunning_ = true;

		initThreadSize_ = initThreadSize;
		curThreadSize_ = initThreadSize;

		// 先构造全部线程对象，保证启动前线程容器已经完整。
		for (int i = 0; i < initThreadSize_; i++)
		{
			auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
			int threadId = ptr->getId();
			threads_.emplace(threadId, std::move(ptr));
		}

		// 启动后，各线程会进入 threadFunc() 等待任务。
		for (int i = 0; i < initThreadSize_; i++)
		{
			threads_[i]->start();
			idleThreadSize_++;
		}
	}

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

private:
	/// 工作线程入口：等待、获取并执行任务。
	void threadFunc(int threadid)
	{
		// 记录最近一次完成任务的时间，用于回收动态创建的空闲线程。
		auto lastTime = std::chrono::high_resolution_clock().now();

		for (;;)
		{
			Task task;
			{
				std::unique_lock<std::mutex> lock(taskQueMtx_);

				std::cout << "tid:" << std::this_thread::get_id()
					<< "尝试获取任务..." << std::endl;

				// 使用循环处理虚假唤醒；只有队列非空时才继续获取任务。
				while (taskQue_.size() == 0)
				{
					// 线程池关闭后，从容器移除当前线程并通知析构线程。
					if (!isPoolRunning_)
					{
						threads_.erase(threadid);
						std::cout << "threadid:" << std::this_thread::get_id() << " exit!"
							<< std::endl;
						exitCond_.notify_all();
						return;
					}

					if (poolMode_ == PoolMode::MODE_CACHED)
					{
						// 每秒检查一次空闲时长，回收超过初始规模的空闲线程。
						if (std::cv_status::timeout ==
							notEmpty_.wait_for(lock, std::chrono::seconds(1)))
						{
							auto now = std::chrono::high_resolution_clock().now();
							auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
							if (dur.count() >= THREAD_MAX_IDLE_TIME
								&& curThreadSize_ > initThreadSize_)
							{
								threads_.erase(threadid);
								curThreadSize_--;
								idleThreadSize_--;

								std::cout << "threadid:" << std::this_thread::get_id() << " exit!"
									<< std::endl;
								return;
							}
						}
					}
					else
					{
						// 固定模式无需定时检查，持续等待新任务或关闭通知。
						notEmpty_.wait(lock);
					}
				}

				idleThreadSize_--;

				std::cout << "tid:" << std::this_thread::get_id()
					<< "获取任务成功..." << std::endl;

				// 取出一个任务并更新队列状态。
				task = taskQue_.front();
				taskQue_.pop();
				taskSize_--;

				// 队列中仍有任务时，继续唤醒其他空闲线程。
				if (taskQue_.size() > 0)
				{
					notEmpty_.notify_all();
				}

				// 队列已释放一个位置，唤醒可能被阻塞的任务提交者。
				notFull_.notify_all();
			} // 执行任务前释放队列锁，避免耗时任务阻塞其他线程访问队列。

			if (task != nullptr)
			{
				task();
			}

			idleThreadSize_++;
			lastTime = std::chrono::high_resolution_clock().now();
		}
	}

	/// 返回线程池是否已经启动且尚未关闭。
	bool checkRunningState() const
	{
		return isPoolRunning_;
	}

private:
	std::unordered_map<int, std::unique_ptr<Thread>> threads_; ///< 逻辑编号到线程对象的映射。

	int initThreadSize_;                 ///< 线程池启动时创建的线程数。
	int threadSizeThreshHold_;           ///< MODE_CACHED 模式的最大线程数。
	std::atomic_int curThreadSize_;       ///< 当前工作线程总数。
	std::atomic_int idleThreadSize_;      ///< 当前空闲线程数。

	/// 线程池内部统一使用的无参数、无返回值任务类型。
	using Task = std::function<void()>;
	std::queue<Task> taskQue_;            ///< 等待执行的任务队列。
	std::atomic_int taskSize_;            ///< 当前排队任务数。
	int taskQueMaxThreshHold_;            ///< 任务队列容量上限。

	std::mutex taskQueMtx_;               ///< 保护任务队列和线程容器。
	std::condition_variable notFull_;     ///< 通知提交者任务队列已有空位。
	std::condition_variable notEmpty_;    ///< 通知工作线程任务队列已有任务。
	std::condition_variable exitCond_;    ///< 通知析构线程所有工作线程已退出。

	PoolMode poolMode_;                   ///< 当前工作模式。
	std::atomic_bool isPoolRunning_;      ///< 线程池是否处于运行状态。
};

#endif
