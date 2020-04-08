#include <stdio.h>
#include <thread>
#include <iostream>

#include "../common/CycleTimer.h"

using namespace std;

typedef struct {
    float x0, x1;
    float y0, y1;
    unsigned int width;
    unsigned int height;
    int maxIterations;
    int* output;
    int threadId;
    int numThreads;
} WorkerArgs;


extern void mandelbrotSerial(
    float x0, float y0, float x1, float y1,
    int width, int height,
    int startRow, int numRows,
    int maxIterations,
    int output[]);

extern void mandelbrotSingleThread(
        float x0, float y0, float x1, float y1,
        int width, int height,
        int startRow, int totalRows, int numThreads,
        int maxIterations,
        int output[]);
//
// workerThreadStart --
//
// Thread entrypoint.
void workerThreadStart(WorkerArgs * const args) {

    double minSerial = 1e30;
    double startTime = CycleTimer::currentSeconds();

    int startRow = args->threadId;
    int totalRows = args->height - startRow;

    mandelbrotSingleThread(args->x0, args->y0, args->x1, args->y1, args->width, args->height,
                     startRow, totalRows, args->numThreads, args->maxIterations, args->output);
    double endTime = CycleTimer::currentSeconds();
    minSerial = std::min(minSerial, endTime - startTime);
    printf("[mandelbrot thead %d]: [%.3f] ms\n", args->threadId, minSerial * 1000);
}

void mandelbrotPartition(int numThreads, int kThread,
                         float x0, float y0, float x1, float y1,
                         int width, int height, int output[], WorkerArgs &args){

    float thread_y0 = y0, thread_y1 = y1;
    int partitionHeight = height;

    if (kThread >= numThreads) {
        cout << "kThread should be < numThreads" << endl;
    } else {
        partitionHeight = height/numThreads + 1;
        thread_y0 = y0 + (y1 - y0)/float(height) * float(partitionHeight * kThread);
        thread_y1 = y0 + (y1 - y0)/float(height) *float(partitionHeight * (kThread+1));

        args.output = output + kThread*partitionHeight*width;
        if (kThread == numThreads - 1){
            thread_y1 = y1;
            partitionHeight = height - partitionHeight * kThread;
        }
    }

    args.x0 = x0;
    args.y0 = thread_y0;
    args.x1 = x1;
    args.y1 = thread_y1;
    args.width = width;
    args.height = partitionHeight;
    args.numThreads = numThreads;

    cout << "dist: " << args.output - output << endl;
    args.threadId = kThread;

}

//
// MandelbrotThread --
//
// Multi-threaded implementation of mandelbrot set image generation.
// Threads of execution are created by spawning std::threads.
void mandelbrotThread(
    int numThreads,
    float x0, float y0, float x1, float y1,
    int width, int height,
    int maxIterations, int output[])
{
    static constexpr int MAX_THREADS = 32;

    if (numThreads > MAX_THREADS)
    {
        fprintf(stderr, "Error: Max allowed threads is %d\n", MAX_THREADS);
        exit(1);
    }

    // Creates thread objects that do not yet represent a thread.
    std::thread workers[MAX_THREADS];
    WorkerArgs args[MAX_THREADS];

    for (int i=0; i<numThreads; i++) {
        args[i].x0 = x0;
        args[i].y0 = y0;
        args[i].x1 = x1;
        args[i].y1 = y1;
        args[i].width = width;
        args[i].height = height;
        args[i].maxIterations = maxIterations;
        args[i].numThreads = numThreads;
        args[i].output = output;

        args[i].threadId = i;
    }

    // Spawn the worker threads.  Note that only numThreads-1 std::threads
    // are created and the main application thread is used as a worker
    // as well.
    for (int i=1; i<numThreads; i++) {
        workers[i] = std::thread(workerThreadStart, &args[i]);
    }
    
    workerThreadStart(&args[0]);
    // join worker threads
    for (int i=1; i<numThreads; i++) {
        workers[i].join();
    }
}

