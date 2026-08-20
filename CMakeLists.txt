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

add_library(mq_protocol src/protocol/protocol_codec.cc)
target_include_directories(mq_protocol PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(mq_core
  src/core/logger.cc
  src/core/memory_pool.cc
  src/core/queue_manager.cc
  src/core/storage/storage_engine.cc
  src/core/thread_pool.cc
  src/core/topic_metadata_store.cc
  src/core/topic.cc
)
target_include_directories(mq_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(mq_server src/server/broker.cc)
target_include_directories(mq_server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mq_server PUBLIC mq_core mq_protocol)

add_library(mq_network
  src/network/buffer.cc
  src/network/event_loop.cc
  src/network/tcp_connection.cc
  src/network/tcp_server.cc
)
target_include_directories(mq_network PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mq_network PUBLIC mq_core mq_protocol)
if(WIN32)
  target_link_libraries(mq_network PUBLIC ws2_32)
endif()

if(MQ_BUILD_TESTS)
  enable_testing()
  add_executable(mq_core_tests test/unit/core_protocol_storage_test.cc)
  target_link_libraries(mq_core_tests PRIVATE mq_core mq_protocol)
  target_include_directories(mq_core_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_core_tests COMMAND mq_core_tests)

  add_executable(mq_concurrency_tests test/unit/concurrency_test.cc)
  target_link_libraries(mq_concurrency_tests PRIVATE mq_core)
  target_include_directories(mq_concurrency_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_concurrency_tests COMMAND mq_concurrency_tests)

  add_executable(mq_broker_tests test/unit/broker_test.cc)
  target_link_libraries(mq_broker_tests PRIVATE mq_server)
  target_include_directories(mq_broker_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_broker_tests COMMAND mq_broker_tests)

  add_executable(mq_event_loop_tests test/unit/event_loop_test.cc)
  target_link_libraries(mq_event_loop_tests PRIVATE mq_network)
  target_include_directories(mq_event_loop_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_event_loop_tests COMMAND mq_event_loop_tests)

  add_executable(mq_network_buffer_tests test/unit/network_buffer_connection_test.cc)
  target_link_libraries(mq_network_buffer_tests PRIVATE mq_network)
  target_include_directories(mq_network_buffer_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_network_buffer_tests COMMAND mq_network_buffer_tests)

  add_executable(mq_logger_tests test/unit/logger_test.cc)
  target_link_libraries(mq_logger_tests PRIVATE mq_core)
  target_include_directories(mq_logger_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_logger_tests COMMAND mq_logger_tests)

  add_executable(mq_tcp_integration_tests test/integration/tcp_integration_test.cc)
  target_link_libraries(mq_tcp_integration_tests PRIVATE mq_network mq_protocol)
  target_include_directories(mq_tcp_integration_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME tcp_integration_tests COMMAND mq_tcp_integration_tests)
endif()
