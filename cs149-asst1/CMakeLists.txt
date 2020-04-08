cmake_minimum_required(VERSION 3.15)
project(cs149_asst1)

set(CMAKE_CXX_STANDARD 14)
include_directories(../common)

add_executable(cs149_asst1 prog5_saxpy/main.cpp prog5_saxpy/saxpySerial.cpp
        common/ppm.cpp common/tasksys.cpp)

#add_executable(cs149_asst1 prog4_sqrt/main.cpp prog4_sqrt/sqrtSerial.cpp
#        common/ppm.cpp common/tasksys.cpp)

#add_executable(cs149_asst1 prog3_mandelbrot_ispc/main.cpp prog3_mandelbrot_ispc/mandelbrotSerial.cpp
#        common/ppm.cpp common/tasksys.cpp)

#add_executable(cs149_asst1 prog1_mandelbrot_threads/main.cpp
#        prog1_mandelbrot_threads/mandelbrotSerial.cpp prog1_mandelbrot_threads/mandelbrotThread.cpp
#        common/ppm.cpp common/tasksys.cpp)