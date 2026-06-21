#include "threadpool.h"

#include <functional>
#include <thread>
#include <iostream>

const int TASK_MAX_THRESHHOLD = INT32_MAX; // 默认任务队列容量。
const int THREAD_MAX_THRESHHOLD = 1024;    // 动态模式的默认线程数上限。
const int THREAD_MAX_IDLE_TIME = 60;       // 额外线程的最大空闲时间，单位：秒。

// 初始化线程池配置和运行时计数器；此时尚未创建工作线程。
ThreadPool::ThreadPool()
	: initThreadSize_(0)
	, taskSize_(0)
	, idleThreadSize_(0)
	, curThreadSize_(0)
	, taskQueMaxThreshHold_(TASK_MAX_THRESHHOLD)
	, threadSizeThreshHold_(THREAD_MAX_THRESHHOLD)
	, poolMode_(PoolMode::MODE_FIXED)
	, isPoolRunning_(false)
{}

// 通知所有工作线程退出，并等待线程容器清空后再完成析构。
ThreadPool::~ThreadPool()
{
	isPoolRunning_ = false;

	std::unique_lock<std::mutex> lock(taskQueMtx_);
	// 唤醒正在等待任务的线程，使其检查关闭状态。
	notEmpty_.notify_all();
	exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
}

// 配置项只允许在线程池启动前修改。
void ThreadPool::setMode(PoolMode mode)
{
	if (checkRunningState())
		return;
	poolMode_ = mode;
}

void ThreadPool::setTaskQueMaxThreshHold(int threshhold)
{
	if (checkRunningState())
		return;
	taskQueMaxThreshHold_ = threshhold;
}

void ThreadPool::setThreadSizeThreshHold(int threshhold)
{
	if (checkRunningState())
		return;
	if (poolMode_ == PoolMode::MODE_CACHED)
	{
		threadSizeThreshHold_ = threshhold;
	}
}

// 将任务放入等待队列，并返回用于取得异步结果的 Result。
Result ThreadPool::submitTask(std::shared_ptr<Task> sp)
{
	// taskQueMtx_ 同时保护任务队列和线程对象容器。
	std::unique_lock<std::mutex> lock(taskQueMtx_);

	// 队列已满时最多等待 1 秒，避免提交线程永久阻塞。
	if (!notFull_.wait_for(lock, std::chrono::seconds(1),
		[&]()->bool { return taskQue_.size() < (size_t)taskQueMaxThreshHold_; }))
	{
		std::cerr << "task queue is full, submit task fail." << std::endl;
		return Result(sp, false);
	}

	// 入队后更新任务计数，并唤醒等待任务的工作线程。
	taskQue_.emplace(sp);
	taskSize_++;
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

	return Result(sp);
}

// 创建并启动指定数量的初始工作线程。
void ThreadPool::start(int initThreadSize)
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

// 工作线程主循环：等待任务、取出任务、释放队列锁，然后执行任务。
void ThreadPool::threadFunc(int threadid)
{
	// 记录最近一次完成任务的时间，用于回收动态创建的空闲线程。
	auto lastTime = std::chrono::high_resolution_clock().now();

	for (;;)
	{
		std::shared_ptr<Task> task;
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
			task->exec();
		}

		idleThreadSize_++;
		lastTime = std::chrono::high_resolution_clock().now();
	}
}

bool ThreadPool::checkRunningState() const
{
	return isPoolRunning_;
}

// ============================== Thread ==============================

int Thread::generateId_ = 0;

Thread::Thread(ThreadFunc func)
	: func_(func)
	, threadId_(generateId_++)
{}

Thread::~Thread() {}

void Thread::start()
{
	std::thread t(func_, threadId_);
	// 线程生命周期由线程池的退出协议管理，不通过 Thread 对象执行 join。
	t.detach();
}

int Thread::getId()const
{
	return threadId_;
}


// =============================== Task ===============================

Task::Task()
	: result_(nullptr)
{}

void Task::exec()
{
	if (result_ != nullptr)
	{
		// 多态调用派生任务的 run()，并把结果交给关联的 Result。
		result_->setVal(run());
	}
}

void Task::setResult(Result* res)
{
	result_ = res;
}

// ============================== Result ==============================

Result::Result(std::shared_ptr<Task> task, bool isValid)
	: isValid_(isValid)
	, task_(task)
{
	task_->setResult(this);
}

Any Result::get()
{
	if (!isValid_)
	{
		return "";
	}
	// 任务尚未完成时阻塞，直到工作线程通过 setVal() 发出通知。
	sem_.wait();
	return std::move(any_);
}

void Result::setVal(Any any)
{
	this->any_ = std::move(any);
	sem_.post();
}
