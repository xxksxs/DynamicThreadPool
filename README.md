# DynamicThreadPool

一个使用 C++17 实现的动态线程池学习项目，包含基础版、优化版和最终版三套实现，用于展示线程池从自定义任务模型到通用 `std::future` 接口，再到安全关闭与动态扩缩容的完整演进过程。

## 功能特性

最终版线程池支持：

- 固定线程数与动态缓存两种工作模式
- 提交普通函数、Lambda、成员函数及任意数量参数
- 通过 `std::future` 获取任务返回值
- 支持 `void`、普通值和移动类型参数
- 自动传播任务执行过程中抛出的异常
- 有界任务队列与提交超时
- Cached 模式下按任务压力扩容
- 使用 `steady_clock` 回收长时间空闲的额外线程
- 排空任务后安全关闭
- 统一 `join` 工作线程，不使用分离线程
- AddressSanitizer 和 ThreadSanitizer 构建选项

## 项目结构

```text
.
├── CMakeLists.txt
├── 线程池项目/                 # 基础版：Task、Any、Result 自定义任务模型
│   ├── threadpool.h
│   ├── threadpool.cpp
│   └── 线程池项目测试.cpp
├── 线程池项目-优化版/          # 优化版：模板 submitTask + std::future
│   ├── threadpool.h
│   └── 线程池项目-优化版.cpp
└── 线程池项目-最终版/          # 推荐使用的最终实现
    ├── threadpool.h
    ├── 线程池项目-最终版.cpp
    └── threadpool_test.cpp
```

三个版本的主要区别：

| 版本 | 任务接口 | 返回值 | 线程退出方式 | 定位 |
| --- | --- | --- | --- | --- |
| 基础版 | 继承 `Task` 并重写 `run()` | 自定义 `Any` / `Result` | `detach()` | 学习类型擦除和任务抽象 |
| 优化版 | 模板 `submitTask()` | `std::future` | `detach()` | 学习可变参数模板和 `packaged_task` |
| 最终版 | 通用模板接口 | `std::future` | 统一 `join()` | 安全关闭、异常传播、动态扩缩容 |

## 环境要求

- 支持 C++17 的编译器
- CMake 3.16 或更高版本
- 支持标准线程库的平台

已在 macOS Apple Clang 环境下完成构建和测试。

## 构建项目

在项目根目录执行：

```bash
cmake -S . -B build
cmake --build build -j
```

构建完成后会生成以下目标：

| 可执行程序 | 说明 |
| --- | --- |
| `thread_pool_basic` | 基础版示例 |
| `thread_pool_optimized` | 优化版示例 |
| `thread_pool_final` | 最终版示例 |
| `thread_pool_final_tests` | 最终版自动测试 |

## 运行示例

运行最终版：

```bash
./build/thread_pool_final
```

也可以运行其他版本：

```bash
./build/thread_pool_basic
./build/thread_pool_optimized
```

基础版程序结尾使用了 `getchar()`，可能需要按回车键退出。

## 最终版快速使用

最终版是头文件实现，只需包含：

```cpp
#include "threadpool.h"
```

### 固定线程模式

```cpp
ThreadPool pool;
pool.start(4);

auto result = pool.submitTask(
    [](int left, int right)
    {
        return left + right;
    },
    20,
    22);

std::cout << result.get() << '\n';
pool.shutdown();
```

### 动态缓存模式

```cpp
ThreadPool pool;
pool.setMode(PoolMode::MODE_CACHED);
pool.setThreadSizeThreshHold(8);
pool.setThreadIdleTimeout(std::chrono::seconds(10));
pool.start(2);
```

当排队任务数量超过空闲线程数量时，线程池会按需创建额外线程，但不会超过配置的线程上限。额外线程持续空闲超过指定时间后会自动退出。

## 公共接口

### 配置接口

配置必须在 `start()` 之前完成：

```cpp
void setMode(PoolMode mode);
void setTaskQueMaxThreshHold(std::size_t size);
void setThreadSizeThreshHold(std::size_t size);
void setThreadIdleTimeout(std::chrono::milliseconds timeout);
```

### 生命周期接口

```cpp
void start(
    std::size_t initialThreadCount =
        std::thread::hardware_concurrency());

void shutdown();
```

`shutdown()` 会执行以下操作：

1. 停止接受新任务。
2. 等待队列中的任务全部执行完成。
3. 唤醒并退出所有工作线程。
4. 对所有线程执行 `join()`。

`shutdown()` 可以重复调用；析构函数也会自动调用它。

### 提交任务

```cpp
template<class Func, class... Args>
auto submitTask(Func&& func, Args&&... args)
    -> std::future<
        std::invoke_result_t<
            std::decay_t<Func>,
            std::decay_t<Args>...>>;
```

任务及其参数会被移动或复制到任务队列中。如果需要传递引用，应使用：

```cpp
int value = 40;

auto future = pool.submitTask(
    [](int& number)
    {
        number += 2;
    },
    std::ref(value));
```

## 状态与异常

线程池包含以下生命周期状态：

```cpp
ThreadPoolState::Created
ThreadPoolState::Running
ThreadPoolState::Stopping
ThreadPoolState::Stopped
```

可以通过以下接口查询：

```cpp
pool.state();
pool.currentThreadCount();
pool.idleThreadCount();
```

提交失败通过返回的 `future` 传播：

- `ThreadPoolStopped`：线程池未启动、正在关闭或已经关闭。
- `TaskQueueFull`：任务队列持续满载超过 1 秒。
- 任务自身抛出的异常：调用 `future::get()` 时原样重新抛出。

示例：

```cpp
auto future = pool.submitTask([] { return 42; });

try
{
    std::cout << future.get() << '\n';
}
catch (const TaskQueueFull& exception)
{
    std::cerr << exception.what() << '\n';
}
```

## 运行测试

```bash
ctest --test-dir build --output-on-failure
```

测试覆盖：

- 固定模式任务执行和结果返回
- 普通函数、Lambda、成员函数和移动参数
- 引用参数与 `void` 返回值
- 任务异常传播
- 队列满异常
- 启动、关闭和重复调用行为
- 析构时排空任务
- Cached 模式扩容与缩容
- 连续创建多个线程池

## Sanitizer 检查

### AddressSanitizer

```bash
cmake -S . -B build-asan \
    -DTHREAD_POOL_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

### ThreadSanitizer

```bash
cmake -S . -B build-tsan \
    -DTHREAD_POOL_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

ASan 和 TSan 不能在同一个构建目录中同时启用。

## 设计说明

最终版在线程池内部使用：

- `std::queue<std::function<void()>>` 保存统一任务
- `std::packaged_task` 连接任务与 `future`
- 互斥锁保护任务队列、线程容器、状态和计数
- 条件变量阻塞工作线程和队列满时的提交线程
- 核心线程持续等待任务
- Cached 模式额外线程使用带超时的条件变量等待

任务执行发生在互斥锁之外，避免耗时任务阻塞其他线程访问任务队列。

## 注意事项

- 配置接口应在 `start()` 之前调用。
- 一个线程池对象只能启动一次。
- 不要在线程池自身的工作任务中销毁该线程池。
- `future::get()` 通常只能调用一次。
- 最终版推荐用于继续学习和扩展；基础版与优化版主要用于对照线程池的演进过程。
