cmake_minimum_required(VERSION 3.16)

project(ThreadPoolExamples LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# 基础版线程池：声明与实现分离。
add_executable(thread_pool_basic
    "线程池项目/线程池项目测试.cpp"
    "线程池项目/threadpool.cpp"
)
target_include_directories(thread_pool_basic PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/线程池项目"
)
target_link_libraries(thread_pool_basic PRIVATE Threads::Threads)

# 优化版线程池：实现位于头文件中。
add_executable(thread_pool_optimized
    "线程池项目-优化版/线程池项目-优化版.cpp"
)
target_include_directories(thread_pool_optimized PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/线程池项目-优化版"
)
target_link_libraries(thread_pool_optimized PRIVATE Threads::Threads)
