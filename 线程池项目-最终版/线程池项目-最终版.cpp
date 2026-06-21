// 最终版线程池示例：安全关闭、异常传播与动态线程管理。

#include "threadpool.h"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main()
{
    ThreadPool pool;
    pool.setMode(PoolMode::MODE_CACHED);
    pool.setThreadSizeThreshHold(4);
    pool.setThreadIdleTimeout(std::chrono::seconds(2));
    pool.start(2);

    auto sum = pool.submitTask(
        [](int begin, int end)
        {
            long long result = 0;
            for (int value = begin; value <= end; ++value)
            {
                result += value;
            }
            return result;
        },
        1,
        100);

    auto message = pool.submitTask(
        [](std::string name)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return "hello, " + name;
        },
        std::string("thread pool"));

    std::cout << "1 到 100 的和: " << sum.get() << '\n';
    std::cout << message.get() << '\n';

    pool.shutdown();
    return 0;
}
