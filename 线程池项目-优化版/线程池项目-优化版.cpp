// 优化版线程池示例：直接提交可调用对象，并通过 std::future 获取结果。

#include <iostream>
#include <functional>
#include <thread>
#include <future>
#include <chrono>
using namespace std;

#include "threadpool.h"


/// 模拟一个耗时的双参数求和任务。
int sum1(int a, int b)
{
    this_thread::sleep_for(chrono::seconds(2));
    return a + b;
}

/// 模拟一个耗时的三参数求和任务。
int sum2(int a, int b, int c)
{
    this_thread::sleep_for(chrono::seconds(2));
    return a + b + c;
}
/// I/O 线程入口示例，listenfd 表示监听套接字描述符。
void io_thread(int listenfd)
{

}

/// 工作线程入口示例，clientfd 表示客户端套接字描述符。
void worker_thread(int clientfd)
{

}
int main()
{
    ThreadPool pool;
    // 如需测试动态扩容，可在 start() 前启用 MODE_CACHED。
    // pool.setMode(PoolMode::MODE_CACHED);
    pool.start(2);

    // 普通函数、不同参数数量的函数和 lambda 均可直接提交。
    future<int> r1 = pool.submitTask(sum1, 1, 2);
    future<int> r2 = pool.submitTask(sum2, 1, 2, 3);
    future<int> r3 = pool.submitTask([](int b, int e)->int {
        int sum = 0;
        for (int i = b; i <= e; i++)
            sum += i;
        return sum;
        }, 1, 100);
    future<int> r4 = pool.submitTask([](int b, int e)->int {
        int sum = 0;
        for (int i = b; i <= e; i++)
            sum += i;
        return sum;
        }, 1, 100);
    future<int> r5 = pool.submitTask([](int b, int e)->int {
        int sum = 0;
        for (int i = b; i <= e; i++)
            sum += i;
        return sum;
        }, 1, 100);
    // future::get() 会等待对应任务完成并返回执行结果。
    cout << r1.get() << endl;
    cout << r2.get() << endl;
    cout << r3.get() << endl;
    cout << r4.get() << endl;
    cout << r5.get() << endl;

}
