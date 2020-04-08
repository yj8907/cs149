#include "tasksys.h"
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads):
ITaskSystem(num_threads),num_threads(num_threads), num_thread_exits(0), bulk_task_id(0), num_idle_workers(0), run_threads(true)
{

//    cout << "test1" << endl;

    this->worker_mtx = vector<mutex>(num_threads);
    this->worker_cv = vector<condition_variable_any>(num_threads);

    vector<int> worker_ids;
    for (int i = 0;i < this->num_threads; i++) worker_ids.push_back(i);

    thread workers[this->num_threads];
    for (int i = 0;i < this->num_threads; i++){
        workers[i] = thread(&TaskSystemParallelThreadPoolSleeping::worker, this, &worker_ids[i]);
    }

    thread schedulers[1];
    schedulers[0] = thread(&TaskSystemParallelThreadPoolSleeping::scheduler, this);

    for (auto &t: schedulers) {
        if (t.joinable()) t.detach();
    }

    for (auto &t: workers) {
        if (t.joinable()) t.detach();
    }

//    cout << "test2" << endl;
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    this->run_threads = false;

    scheduler_mtx.lock();
    scheduler_cv.notify_all();
    scheduler_mtx.unlock();

    for (int i = 0;i < num_threads; i++){
        worker_mtx[i].lock();
        worker_cv[i].notify_all();
        worker_mtx[i].unlock();
    }

    while(true){
        if (this->num_thread_exits.load(memory_order_relaxed) >= this->num_threads + 1)
            break;
    }

}

void TaskSystemParallelThreadPoolSleeping::push_tasks(TaskID tid){

    auto taskinfo_iter = taskInfo.find(tid);
    assert(taskinfo_iter != taskInfo.end());

    int num_total_tasks = taskinfo_iter->second.second;
    worker_shared_mtx.lock();
    for (int i = 0;i < num_total_tasks; ++i){
        readyQueue.emplace(pair<TaskID, IRunnable*>(taskinfo_iter->first, taskinfo_iter->second.first),
                           pair<int, int>(i, num_total_tasks));
    }
    worker_shared_mtx.unlock();
}


bool TaskSystemParallelThreadPoolSleeping::checkDeps(TaskID tid) {

    auto iter = taskDownStreamDeps.find(tid);
    if (iter == taskDownStreamDeps.end()){
        return true;
    } else {
        for (auto & prev_task : iter->second){
            if (finishedTaskSet.find(prev_task) == finishedTaskSet.end())
                return false;
        }
    }
    return true;
}



void TaskSystemParallelThreadPoolSleeping::scheduler(){
//    cout << "enter scheduler" << endl;

    while (this->run_threads){

        scheduler_mtx.lock();
        while (!finishedTaskQueue.empty()){
            TaskID finished_task = finishedTaskQueue.front();
            finishedTaskQueue.pop();

            finishedTaskSet.insert(finished_task);
            auto iter = taskUpStreamDeps.find(finished_task);
            if (iter != taskUpStreamDeps.end()) {
                for (auto t : iter->second){
                    pendingTaskQueue.push(t);
                }
            }
        }

        while (!pendingTaskQueue.empty()){
            TaskID pending_task = pendingTaskQueue.front();
            pendingTaskQueue.pop();
            if (checkDeps(pending_task)) push_tasks(pending_task);
        }

        for (int i = 0;i < this->num_threads; i++){
            this->worker_mtx[i].lock();
            this->worker_cv[i].notify_all();
            this->worker_mtx[i].unlock();
        }

        master_cv.notify_all();

        while(finishedTaskQueue.empty() && pendingTaskQueue.empty() && this->run_threads){
            scheduler_cv.wait(scheduler_mtx, [this](){
                return !finishedTaskQueue.empty() || !pendingTaskQueue.empty() || !this->run_threads;
            });
        }
        scheduler_mtx.unlock();
    }

    num_thread_exits++;

}

void TaskSystemParallelThreadPoolSleeping::worker(const int* worker_id) {
//    cout << "enter worker" << endl;
    const int wid = *worker_id;

    while (this->run_threads) {

        worker_shared_mtx.lock();

        if ( !readyQueue.empty()){
            pair<pair<TaskID, IRunnable*>, pair<int, int>> curr_work = readyQueue.front();
            readyQueue.pop();
            worker_shared_mtx.unlock();

            pair<TaskID, IRunnable*> curr_task = curr_work.first;
            pair<int, int> curr_task_params = curr_work.second;
            curr_task.second->runTask(curr_task_params.first, curr_task_params.second);

            worker_shared_mtx.lock();
            taskProgress[curr_task.first]++;
            if (taskProgress[curr_task.first] == curr_task_params.second){
                worker_shared_mtx.unlock();

                scheduler_mtx.lock();
                finishedTaskQueue.push(curr_task.first);
                scheduler_cv.notify_all();
                scheduler_mtx.unlock();
            } else {
                worker_shared_mtx.unlock();
            }

        } else {
            worker_shared_mtx.unlock();
            num_idle_workers++;

            this->worker_mtx[wid].lock();
            while( readyQueue.empty() && this->run_threads) {
                this->worker_cv[wid].wait(this->worker_mtx[wid], [this](){
                    return ( !readyQueue.empty() || !this->run_threads); });
            }
            this->worker_mtx[wid].unlock();

            num_idle_workers--;
        }
    }
    num_thread_exits += 1;
}



void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    vector<TaskID> deps;
    runAsyncWithDeps(runnable, num_total_tasks, deps);
    sync();

}



TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {


    TaskID new_task_id = bulk_task_id++;

    scheduler_mtx.lock();

    taskInfo.emplace(new_task_id, pair<IRunnable*, int>(runnable, num_total_tasks));
    for (auto &prev_task : deps){
        taskUpStreamDeps[prev_task].insert(new_task_id);
        taskDownStreamDeps[new_task_id].insert(prev_task);
    }

    pendingTaskQueue.push(new_task_id);

    worker_shared_mtx.lock();
    taskProgress[new_task_id] = 0;
    worker_shared_mtx.unlock();

    scheduler_cv.notify_all();
    scheduler_mtx.unlock();

    return new_task_id;

}

void TaskSystemParallelThreadPoolSleeping::sync() {

    scheduler_mtx.lock();
    while( finishedTaskSet.size() != bulk_task_id )
        master_cv.wait(this->scheduler_mtx, [this](){
            return finishedTaskSet.size() == bulk_task_id;
        });
    scheduler_mtx.unlock();

}

