#include "tasksys.h"
#include <vector>
#include <thread>
#include <iostream>

using namespace std;

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemSerial::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->task_id = 0;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::worker(IRunnable *runnable, int num_total_tasks) {

    while (this->task_id.load(memory_order_relaxed) < num_total_tasks){
        int curr_id = this->task_id.fetch_add(1, memory_order_relaxed);
        if (curr_id < num_total_tasks) runnable->runTask(curr_id, num_total_tasks);
    }

}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    this->task_id = 0;
    vector<thread> workers;
    for (int i = 0; i < this->num_threads; i++) {
        workers.push_back(move(thread(&TaskSystemParallelSpawn::worker,
                                      this, ref(runnable), ref(num_total_tasks))));
    }

    for (auto &t : workers){
        if (t.joinable()) t.join();
    }

}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelSpawn::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->num_thread_exits = 0;

    this->lock.lock();
    this->task_id = 0; this->num_total_tasks = 0; this->num_task_completed = 0;
    this->running = true;
    this->lock.unlock();

    vector<int> worker_ids;
    for (int i = 0;i < this->num_threads; i++) worker_ids.push_back(i);

    thread workers[this->num_threads];
    for (int i = 0;i < this->num_threads; i++){
        int wid = worker_ids[i];
        workers[i] = thread(&TaskSystemParallelThreadPoolSpinning::worker, this, ref(wid));
    }

    for (auto &t: workers) {
        if (t.joinable()) t.detach();
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {

    this->running = false;
    while(true){
        if (this->num_thread_exits.load(memory_order_relaxed) == this->num_threads)
            break;
    }
}

void TaskSystemParallelThreadPoolSpinning::worker(int worker_id) {

    int curr_id;
    while (this->running) {

        this->lock.lock();
        if ((curr_id = this->task_id.fetch_add(1, memory_order_relaxed)) < this->num_total_tasks){
            this->lock.unlock();

            if (curr_id < this->num_total_tasks)
                this -> task->runTask(curr_id, this->num_total_tasks);

            this->lock.lock();
            this->num_task_completed+=1;
            this->lock.unlock();
        } else {
            this->lock.unlock();
        }

    }
    this->num_thread_exits += 1;

}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->lock.lock();
    this->task = runnable;
    this->num_total_tasks = num_total_tasks; this->task_id = 0;
    this->num_task_completed = 0;
    this->lock.unlock();

    while(true){
        if (this->num_task_completed.load(memory_order_relaxed) == num_total_tasks) break;
    }

}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {

    this->num_threads = num_threads;
    this->worker_mtx = vector<mutex>(num_threads);
    this->worker_cv = vector<condition_variable_any>(num_threads);

    this->num_thread_exits = 0;

    this->task_id = 0; this->num_total_tasks = 0; this->num_idle_workers = 0;
    this->run_threads = true;

    vector<int> worker_ids;
    for (int i = 0;i < this->num_threads; i++) worker_ids.push_back(i);

    thread workers[this->num_threads];
    for (int i = 0;i < this->num_threads; i++){
        workers[i] = thread(&TaskSystemParallelThreadPoolSleeping::worker, this, &worker_ids[i]);
    }

    for (auto &t: workers) {
        if (t.joinable()) t.detach();
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    this->run_threads = false;

    for (int i = 0;i < this->num_threads; i++){
        this->worker_mtx[i].lock();
        this->worker_cv[i].notify_all();
        this->worker_mtx[i].unlock();
    }

    while(true){
        if (this->num_thread_exits.load(memory_order_relaxed) == this->num_threads)
            break;
    }
}


void TaskSystemParallelThreadPoolSleeping::worker(const int* worker_id) {

    const int wid = *worker_id;

    int curr_id;
    while (this->run_threads) {

        worker_shared_mtx.lock();

        if ( (curr_id = this->task_id) < this->num_total_tasks){
            this->task_id++;
            worker_shared_mtx.unlock();
            if (curr_id < this->num_total_tasks)
                this -> task->runTask(curr_id, this->num_total_tasks);

        } else {
            worker_shared_mtx.unlock();
            num_idle_workers++;

            master_mtx.lock();
            master_cv.notify_all();
            master_mtx.unlock();

            this->worker_mtx[wid].lock();
            while( this->task_id >= this->num_total_tasks && this->run_threads) {
                this->worker_cv[wid].wait(this->worker_mtx[wid], [this](){
                    return (this->task_id < this->num_total_tasks)
                           || (!this->run_threads); });
            }
            this->worker_mtx[wid].unlock();

            num_idle_workers--;
        }
    }
    this->num_thread_exits += 1;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    worker_shared_mtx.lock();
    this->task = runnable;
    this->num_total_tasks = num_total_tasks; this->task_id = 0;
    worker_shared_mtx.unlock();

    for (int i = 0;i < this->num_threads; i++){
        this->worker_mtx[i].lock();
        this->worker_cv[i].notify_all();
        this->worker_mtx[i].unlock();
    }

    master_mtx.lock();
    while( (this->num_idle_workers.load(memory_order_relaxed) < this->num_threads) ||
           (task_id < this->num_total_tasks) )
        master_cv.wait(this->master_mtx, [this](){
            return (this->num_idle_workers.load(memory_order_relaxed) >= this->num_threads)
                   && (this->task_id >= this->num_total_tasks);
        });
    master_mtx.unlock();

}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
