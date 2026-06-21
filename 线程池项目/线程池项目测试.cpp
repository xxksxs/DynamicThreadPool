// 基础版线程池示例：通过继承 Task 定义任务，并使用 Result 获取返回值。

#include <iostream>
#include <chrono>
#include <thread>
using namespace std;

#include "threadpool.h"

using uLong = unsigned long long;

/**
 * @brief 对闭区间 [begin, end] 中的整数求和。
 *
 * 该任务用于演示自定义 Task、并行计算和任意类型返回值。
 */
class MyTask : public Task
{
public:
    MyTask(int begin, int end)
        : begin_(begin)
        , end_(end)
    {}
    /// 在线程池工作线程中执行区间求和，并返回计算结果。
    Any run() override
    {
        std::cout << "tid:" << std::this_thread::get_id()
            << "begin!" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(3));
        uLong sum = 0;
        for (uLong i = begin_; i <= end_; i++)
            sum += i;
        std::cout << "tid:" << std::this_thread::get_id()
            << "end!" << std::endl;

        return sum;
    }

private:
    int begin_; ///< 求和区间起点，包含该值。
    int end_;   ///< 求和区间终点，包含该值。
};

int main()
{
    {
        ThreadPool pool;
        pool.setMode(PoolMode::MODE_CACHED);
        pool.start(2);

        // 前两个任务保留 Result，后续任务仅用于演示队列和动态扩容。
        Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000000));
        Result res2 = pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
        pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
        pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
        pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));

        // 调用 get() 会等待任务完成，然后从 Any 中取出实际类型。
        // uLong sum1 = res1.get().cast_<uLong>();
        // cout << sum1 << endl;
    } // 离开作用域时，线程池等待全部工作线程退出。

    cout << "main over!" << endl;
    getchar();
#if 0
    // 完整的分段求和示例。设为 1 可参与编译。
    {
        ThreadPool pool;
        pool.setMode(PoolMode::MODE_CACHED);
        pool.start(4);

        Result res1 = pool.submitTask(std::make_shared<MyTask>(1, 100000000));
        Result res2 = pool.submitTask(std::make_shared<MyTask>(100000001, 200000000));
        Result res3 = pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
        pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));

        pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));
        pool.submitTask(std::make_shared<MyTask>(200000001, 300000000));

        uLong sum1 = res1.get().cast_<uLong>();
        uLong sum2 = res2.get().cast_<uLong>();
        uLong sum3 = res3.get().cast_<uLong>();

        // 主线程负责拆分任务、等待结果并汇总各分段的计算值。
        cout << (sum1 + sum2 + sum3) << endl;
    }

    // 单线程求和基准，可用于和线程池版本比较。
    /*uLong sum = 0;
    for (uLong i = 1; i <= 300000000; i++)
        sum += i;
    cout << sum << endl;*/

    getchar();

#endif
}
