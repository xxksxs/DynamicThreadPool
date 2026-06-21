cmake_minimum_required(VERSION 3.16)

project(ThreadPoolExamples LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
include(CTest)

option(THREAD_POOL_ENABLE_ASAN "Enable AddressSanitizer for final targets" OFF)
option(THREAD_POOL_ENABLE_TSAN "Enable ThreadSanitizer for final targets" OFF)

if(THREAD_POOL_ENABLE_ASAN AND THREAD_POOL_ENABLE_TSAN)
    message(FATAL_ERROR "AddressSanitizer and ThreadSanitizer cannot be enabled together")
endif()

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

# 最终版线程池：头文件库实现，支持安全关闭和动态扩缩容。
add_executable(thread_pool_final
    "线程池项目-最终版/线程池项目-最终版.cpp"
)
target_include_directories(thread_pool_final PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/线程池项目-最终版"
)
target_link_libraries(thread_pool_final PRIVATE Threads::Threads)

add_executable(thread_pool_final_tests
    "线程池项目-最终版/threadpool_test.cpp"
)
target_include_directories(thread_pool_final_tests PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/线程池项目-最终版"
)
target_link_libraries(thread_pool_final_tests PRIVATE Threads::Threads)

if(THREAD_POOL_ENABLE_ASAN)
    foreach(target thread_pool_final thread_pool_final_tests)
        target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address)
    endforeach()
endif()

if(THREAD_POOL_ENABLE_TSAN)
    foreach(target thread_pool_final thread_pool_final_tests)
        target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=thread)
    endforeach()
endif()

if(BUILD_TESTING)
    add_test(NAME thread_pool_final_tests COMMAND thread_pool_final_tests)
endif()
