#include "threadpool.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace
{
using namespace std::chrono_literals;

void require(bool condition, const std::string& message)
{
    if (!condition)
    {
        throw std::runtime_error(message);
    }
}

template<class Exception, class Future>
void requireFutureThrows(Future& future, const std::string& message)
{
    try
    {
        future.get();
    }
    catch (const Exception&)
    {
        return;
    }
    catch (...)
    {
        throw std::runtime_error(message + ": unexpected exception type");
    }

    throw std::runtime_error(message + ": exception was not thrown");
}

void testFixedModeAndTaskForms()
{
    struct Calculator
    {
        int multiply(int left, int right) const
        {
            return left * right;
        }
    };

    ThreadPool pool;
    pool.start(2);

    auto sum = pool.submitTask([](int left, int right) { return left + right; }, 20, 22);
    auto member = pool.submitTask(&Calculator::multiply, Calculator{}, 6, 7);
    auto moved = pool.submitTask(
        [](std::unique_ptr<int> value) { return *value; },
        std::make_unique<int>(42));

    int referencedValue = 40;
    auto referenced = pool.submitTask(
        [](int& value)
        {
            value += 2;
            return value;
        },
        std::ref(referencedValue));

    std::atomic_bool voidTaskFinished{false};
    auto voidTask = pool.submitTask([&voidTaskFinished] { voidTaskFinished = true; });

    require(sum.get() == 42, "lambda task returned the wrong result");
    require(member.get() == 42, "member function task returned the wrong result");
    require(moved.get() == 42, "move-only argument was not handled correctly");
    require(referenced.get() == 42, "reference argument was not handled correctly");
    require(referencedValue == 42, "reference argument did not update its source");
    voidTask.get();
    require(voidTaskFinished, "void task did not execute");
}

void testTaskExceptionPropagation()
{
    ThreadPool pool;
    pool.start(1);

    auto future = pool.submitTask(
        []() -> int
        {
            throw std::runtime_error("task failed");
        });

    requireFutureThrows<std::runtime_error>(
        future,
        "task exception was not propagated through future");
}

void testQueueFullException()
{
    ThreadPool pool;
    pool.setTaskQueMaxThreshHold(1);
    pool.start(1);

    std::promise<void> releasePromise;
    std::shared_future<void> release = releasePromise.get_future().share();
    std::promise<void> firstStartedPromise;
    std::future<void> firstStarted = firstStartedPromise.get_future();

    auto running = pool.submitTask(
        [release, promise = std::move(firstStartedPromise)]() mutable
        {
            promise.set_value();
            release.wait();
        });

    firstStarted.wait();
    auto queued = pool.submitTask([] { return 7; });
    auto rejected = pool.submitTask([] { return 9; });

    requireFutureThrows<TaskQueueFull>(
        rejected,
        "full queue did not return TaskQueueFull");

    releasePromise.set_value();
    running.get();
    require(queued.get() == 7, "queued task was lost");
}

void testStateAndShutdownBehavior()
{
    ThreadPool pool;

    auto beforeStart = pool.submitTask([] { return 1; });
    requireFutureThrows<ThreadPoolStopped>(
        beforeStart,
        "submission before start did not fail");

    pool.start(2);

    bool repeatedStartFailed = false;
    try
    {
        pool.start(1);
    }
    catch (const std::logic_error&)
    {
        repeatedStartFailed = true;
    }
    require(repeatedStartFailed, "repeated start did not throw logic_error");

    std::atomic_int completed{0};
    std::vector<std::future<void>> futures;
    for (int index = 0; index < 8; ++index)
    {
        futures.push_back(pool.submitTask(
            [&completed]
            {
                std::this_thread::sleep_for(10ms);
                ++completed;
            }));
    }

    pool.shutdown();
    pool.shutdown();

    for (auto& future : futures)
    {
        future.get();
    }

    require(completed == 8, "shutdown did not drain queued tasks");
    require(pool.state() == ThreadPoolState::Stopped, "pool did not enter Stopped state");
    require(pool.currentThreadCount() == 0, "workers were not joined during shutdown");

    auto afterShutdown = pool.submitTask([] { return 1; });
    requireFutureThrows<ThreadPoolStopped>(
        afterShutdown,
        "submission after shutdown did not fail");
}

void testDestructorDrainsTasks()
{
    std::atomic_int completed{0};
    std::vector<std::future<void>> futures;

    {
        ThreadPool pool;
        pool.start(2);
        for (int index = 0; index < 6; ++index)
        {
            futures.push_back(pool.submitTask(
                [&completed]
                {
                    std::this_thread::sleep_for(10ms);
                    ++completed;
                }));
        }
    }

    for (auto& future : futures)
    {
        future.get();
    }
    require(completed == 6, "destructor did not drain queued tasks");
}

void testCachedModeExpansionAndShrink()
{
    ThreadPool pool;
    pool.setMode(PoolMode::MODE_CACHED);
    pool.setThreadSizeThreshHold(3);
    pool.setThreadIdleTimeout(50ms);
    pool.start(1);

    std::mutex startMutex;
    std::condition_variable startCondition;
    int startedCount = 0;
    std::set<std::thread::id> workerIds;

    std::promise<void> releasePromise;
    std::shared_future<void> release = releasePromise.get_future().share();

    auto blockingTask = [&]
    {
        {
            std::lock_guard<std::mutex> lock(startMutex);
            workerIds.insert(std::this_thread::get_id());
            ++startedCount;
        }
        startCondition.notify_one();
        release.wait();
    };

    std::vector<std::future<void>> futures;
    for (int expected = 1; expected <= 3; ++expected)
    {
        futures.push_back(pool.submitTask(blockingTask));
        std::unique_lock<std::mutex> lock(startMutex);
        require(
            startCondition.wait_for(
                lock,
                1s,
                [&] { return startedCount >= expected; }),
            "cached mode did not create enough workers");
    }

    require(workerIds.size() == 3, "cached mode did not expand to three workers");
    require(pool.currentThreadCount() == 3, "active thread count is incorrect after expansion");

    releasePromise.set_value();
    for (auto& future : futures)
    {
        future.get();
    }

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (pool.currentThreadCount() != 1 &&
           std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(20ms);
    }

    require(pool.currentThreadCount() == 1, "cached workers were not reclaimed");
    require(pool.submitTask([] { return 42; }).get() == 42, "pool failed after shrinking");
}

void testSequentialPools()
{
    for (int iteration = 0; iteration < 3; ++iteration)
    {
        ThreadPool pool;
        pool.start(1);
        auto future = pool.submitTask([iteration] { return iteration; });
        require(future.get() == iteration, "sequential pool returned the wrong value");
    }
}

} // namespace

int main()
{
    struct TestCase
    {
        const char* name;
        void (*function)();
    };

    const std::vector<TestCase> tests{
        {"fixed mode and task forms", testFixedModeAndTaskForms},
        {"task exception propagation", testTaskExceptionPropagation},
        {"queue full exception", testQueueFullException},
        {"state and shutdown behavior", testStateAndShutdownBehavior},
        {"destructor drains tasks", testDestructorDrainsTasks},
        {"cached mode expansion and shrink", testCachedModeExpansionAndShrink},
        {"sequential pools", testSequentialPools},
    };

    int failures = 0;
    for (const auto& test : tests)
    {
        try
        {
            test.function();
            std::cout << "[PASS] " << test.name << '\n';
        }
        catch (const std::exception& exception)
        {
            ++failures;
            std::cerr << "[FAIL] " << test.name << ": " << exception.what() << '\n';
        }
    }

    if (failures != 0)
    {
        std::cerr << failures << " test(s) failed\n";
        return 1;
    }

    std::cout << "All thread pool tests passed\n";
    return 0;
}
