#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <vector>
#include <queue>
#include <memory>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <unordered_map>
#include <chrono>
#include <thread>

/**
 * @brief 简单的类型擦除容器，用于保存任务的任意类型返回值。
 *
 * Any 仅支持移动，不支持复制。调用 cast_() 时，模板参数必须与存入
 * Any 时的实际类型完全一致，否则会抛出异常。
 */
class Any
{
public:
	Any() = default;
	~Any() = default;
	Any(const Any&) = delete;
	Any& operator=(const Any&) = delete;
	Any(Any&&) = default;
	Any& operator=(Any&&) = default;

	/// 将任意类型的数据封装到 Any 中。
	template<typename T>
	Any(T data) : base_(std::make_unique<Derive<T>>(data))
	{}

	/// 按指定类型取出数据；类型不匹配时抛出异常。
	template<typename T>
	T cast_()
	{
		// 借助 RTTI 将类型擦除后的基类指针恢复为具体派生类型。
		Derive<T>* pd = dynamic_cast<Derive<T>*>(base_.get());
		if (pd == nullptr)
		{
			throw "type is unmatch!";
		}
		return pd->data_;
	}
private:
	/// 所有实际数据包装类型的多态基类。
	class Base
	{
	public:
		virtual ~Base() = default;
	};

	/// 保存具体类型数据的包装类。
	template<typename T>
	class Derive : public Base
	{
	public:
		Derive(T data) : data_(data)
		{}
		T data_;  ///< 实际保存的数据。
	};

private:
	std::unique_ptr<Base> base_;  ///< 指向实际数据包装对象。
};

/**
 * @brief 基于互斥锁和条件变量实现的计数信号量。
 *
 * 本项目使用它在工作线程和调用 Result::get() 的线程之间传递
 * “任务结果已就绪”事件。
 */
class Semaphore
{
public:
	Semaphore(int limit = 0)
		:resLimit_(limit)
	{}
	~Semaphore() = default;

	/// 获取一个信号量资源；没有可用资源时阻塞当前线程。
	void wait()
	{
		std::unique_lock<std::mutex> lock(mtx_);
		cond_.wait(lock, [&]()->bool {return resLimit_ > 0; });
		resLimit_--;
	}

	/// 释放一个信号量资源，并唤醒等待线程。
	void post()
	{
		std::unique_lock<std::mutex> lock(mtx_);
		resLimit_++;
		cond_.notify_all();
	}
private:
	int resLimit_;                 ///< 当前可用的信号量资源数量。
	std::mutex mtx_;               ///< 保护 resLimit_。
	std::condition_variable cond_; ///< 等待信号量资源的条件变量。
};

// Result 需要持有任务对象，因此先声明 Task。
class Task;

/**
 * @brief 表示异步任务的执行结果。
 *
 * Result 与一个 Task 关联。调用 get() 会等待任务完成，然后返回封装在
 * Any 中的结果；工作线程通过 setVal() 写入结果并唤醒等待者。
 */
class Result
{
public:
	Result(std::shared_ptr<Task> task, bool isValid = true);
	~Result() = default;

	/// 由工作线程写入任务结果，并通知等待结果的线程。
	void setVal(Any any);

	/// 等待任务完成并取得返回值。
	Any get();
private:
	Any any_;                      ///< 任务返回值。
	Semaphore sem_;                ///< 同步结果生产者和消费者。
	std::shared_ptr<Task> task_;   ///< 与该结果关联的任务对象。
	std::atomic_bool isValid_;     ///< 提交是否成功，结果是否有效。
};

/**
 * @brief 所有可提交任务的抽象基类。
 *
 * 用户通过继承 Task 并实现 run() 定义具体任务。exec() 由线程池调用，
 * 它负责执行 run()，并将返回值写入关联的 Result。
 */
class Task
{
public:
	Task();
	~Task() = default;

	/// 执行任务，并将 run() 的返回值交给 Result。
	void exec();

	/// 绑定用于接收任务返回值的 Result 对象。
	void setResult(Result* res);

	/// 执行具体任务逻辑，由派生类实现。
	virtual Any run() = 0;

private:
	Result* result_;  ///< 非拥有指针，指向与当前任务关联的 Result。
};

/// 线程池的工作模式。
enum class PoolMode
{
	MODE_FIXED,  ///< 固定线程数，运行期间不动态增减。
	MODE_CACHED, ///< 按任务压力动态扩容，并回收长时间空闲的额外线程。
};

/**
 * @brief 工作线程的轻量封装。
 *
 * 每个 Thread 保存一个线程函数和线程池内部使用的逻辑编号。
 * start() 创建并分离一个 std::thread。
 */
class Thread
{
public:
	/// 工作线程入口函数类型，参数为线程的逻辑编号。
	using ThreadFunc = std::function<void(int)>;

	Thread(ThreadFunc func);
	~Thread();

	/// 创建并启动底层线程。
	void start();

	/// 返回线程池内部使用的逻辑编号。
	int getId()const;
private:
	ThreadFunc func_;       ///< 工作线程入口函数。
	static int generateId_; ///< 下一个可分配的逻辑线程编号。
	int threadId_;          ///< 当前线程的逻辑编号。
};

/**
 * @brief 管理工作线程并异步执行 Task 的线程池。
 *
 * 线程池支持固定线程数和动态缓存两种模式。配置接口必须在 start()
 * 之前调用；启动后可通过 submitTask() 提交任务并取得 Result。
 */
class ThreadPool
{
public:
	ThreadPool();
	~ThreadPool();

	/// 设置工作模式；线程池启动后调用无效。
	void setMode(PoolMode mode);

	/// 设置任务队列允许容纳的最大任务数；线程池启动后调用无效。
	void setTaskQueMaxThreshHold(int threshhold);

	/// 设置 MODE_CACHED 模式下允许创建的最大线程数。
	void setThreadSizeThreshHold(int threshhold);

	/// 提交任务；队列持续满 1 秒时返回无效 Result。
	Result submitTask(std::shared_ptr<Task> sp);

	/// 创建初始工作线程并启动线程池。
	void start(int initThreadSize = std::thread::hardware_concurrency());

	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

private:
	/// 工作线程入口：等待、获取并执行任务。
	void threadFunc(int threadid);

	/// 返回线程池是否已经启动且尚未关闭。
	bool checkRunningState() const;

private:
	std::unordered_map<int, std::unique_ptr<Thread>> threads_; ///< 逻辑编号到线程对象的映射。

	int initThreadSize_;                 ///< 线程池启动时创建的线程数。
	int threadSizeThreshHold_;           ///< MODE_CACHED 模式的最大线程数。
	std::atomic_int curThreadSize_;       ///< 当前工作线程总数。
	std::atomic_int idleThreadSize_;      ///< 当前空闲线程数。

	std::queue<std::shared_ptr<Task>> taskQue_; ///< 等待执行的任务队列。
	std::atomic_int taskSize_;                  ///< 当前排队任务数。
	int taskQueMaxThreshHold_;                  ///< 任务队列容量上限。

	std::mutex taskQueMtx_;              ///< 保护任务队列和线程容器。
	std::condition_variable notFull_;    ///< 通知提交者任务队列已有空位。
	std::condition_variable notEmpty_;   ///< 通知工作线程任务队列已有任务。
	std::condition_variable exitCond_;   ///< 通知析构线程所有工作线程已退出。

	PoolMode poolMode_;                  ///< 当前工作模式。
	std::atomic_bool isPoolRunning_;     ///< 线程池是否处于运行状态。
};

#endif
