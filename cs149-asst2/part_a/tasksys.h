#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <mutex>
#include <condition_variable>
#include <atomic>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    void worker(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();

private:
    int num_threads;
    std::atomic<int> task_id;

};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char* name();
    void worker(int worker_id);

    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();

private:
    int num_threads;
    std::atomic<int> num_task_completed;
    std::atomic<int> task_id;

    IRunnable *task;
    int num_total_tasks;
    bool running;

    std::atomic<int> num_thread_exits;
    std::mutex lock;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    void worker(const int* worker_id);

    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();

private:
    int num_threads;
    std::atomic<int> num_idle_workers;
    int task_id;
    int num_total_tasks;

    IRunnable *task;

    bool run_threads;

    std::atomic<int> num_thread_exits;
    std::mutex worker_shared_mtx;
    std::vector<std::mutex> worker_mtx;
    std::mutex master_mtx;

    std::vector<std::condition_variable_any> worker_cv;
    std::condition_variable_any master_cv;
};

#endif
