cmake_minimum_required(VERSION 3.16)
project(mq_project VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 骨架阶段：目录与文档就绪，源码待后续按 docs/architecture.md §9 里程碑填充。
# 一旦开始实现，在此恢复 add_subdirectory(src) 并配置各模块库。

option(MQ_BUILD_TESTS "Build unit & integration tests" OFF)
option(MQ_BUILD_BENCH "Build benchmark binaries" OFF)
option(MQ_BUILD_SERVER "Build broker server binary" ON)