cmake_minimum_required(VERSION 3.16)
project(mq_project VERSION 0.1.0 LANGUAGES CXX)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
  add_compile_options(/utf-8)
endif()

if(UNIX)
  add_compile_options(-pthread)
  add_link_options(-pthread)
endif()

# 当前构建按模块直接声明库和测试目标；可选 TLS、sanitizer 与 libFuzzer 通过下方开关启用。

option(MQ_BUILD_TESTS "Build unit & integration tests" OFF)
option(MQ_BUILD_BENCH "Build benchmark binaries" OFF)
option(MQ_BUILD_TOOLS "Build operational tools" ON)
option(MQ_BUILD_SERVER "Build broker server binary" ON)
option(MQ_ENABLE_SANITIZERS "Enable ASan and UBSan for GCC/Clang" OFF)
option(MQ_ENABLE_TLS "Enable OpenSSL TLS transport" OFF)
option(MQ_BUILD_FUZZERS "Build Clang libFuzzer targets" OFF)

if(MQ_ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

add_library(mq_protocol src/protocol/protocol_codec.cc)
target_include_directories(mq_protocol PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(MQ_BUILD_FUZZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "MQ_BUILD_FUZZERS requires a Clang compiler with libFuzzer support")
  endif()
  add_executable(mq_protocol_libfuzzer test/fuzz/protocol_fuzz.cc)
  target_link_libraries(mq_protocol_libfuzzer PRIVATE mq_protocol)
  target_include_directories(mq_protocol_libfuzzer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  target_compile_options(mq_protocol_libfuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(mq_protocol_libfuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
endif()

add_library(mq_tls src/network/tls.cc)
target_include_directories(mq_tls PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
if(MQ_ENABLE_TLS)
  find_package(OpenSSL REQUIRED)
  target_compile_definitions(mq_tls PUBLIC MQ_ENABLE_TLS=1)
  target_include_directories(mq_tls PUBLIC ${OPENSSL_INCLUDE_DIR})
  target_link_libraries(mq_tls PUBLIC ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
endif()

add_library(mq_core
  src/core/logger.cc
  src/core/consumer_offset_store.cc
  src/core/memory_pool.cc
  src/core/queue_manager.cc
  src/core/storage/storage_engine.cc
  src/core/thread_pool.cc
  src/core/topic_metadata_store.cc
  src/core/topic.cc
)
target_include_directories(mq_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(mq_server src/server/broker.cc src/server/replication.cc src/server/replication_client.cc)
target_include_directories(mq_server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mq_core PUBLIC Threads::Threads)
target_link_libraries(mq_server PUBLIC mq_core mq_protocol mq_tls)
if(WIN32)
  target_link_libraries(mq_server PUBLIC ws2_32)
endif()

add_library(mq_client src/client/mq_client.cc)
target_include_directories(mq_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mq_client PUBLIC mq_core mq_protocol mq_tls Threads::Threads)
if(WIN32)
  target_link_libraries(mq_client PUBLIC ws2_32)
endif()

if(MQ_BUILD_SERVER)
  add_executable(mq_broker src/server/main.cc)
  target_link_libraries(mq_broker PRIVATE mq_server mq_network)
  target_include_directories(mq_broker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endif()

if(MQ_BUILD_BENCH)
  add_executable(mq_bench bench/mq_bench.cc)
  target_link_libraries(mq_bench PRIVATE mq_client Threads::Threads)
  target_include_directories(mq_bench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endif()

if(MQ_BUILD_TOOLS)
  add_executable(mq_admin tools/mq_admin.cc)
  target_link_libraries(mq_admin PRIVATE mq_client)
  target_include_directories(mq_admin PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
endif()

add_library(mq_network
  src/network/buffer.cc
  src/network/event_loop.cc
  src/network/tcp_connection.cc
  src/network/tcp_server.cc
)
target_include_directories(mq_network PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mq_network PUBLIC mq_core mq_protocol mq_tls Threads::Threads)
if(WIN32)
  target_link_libraries(mq_network PUBLIC ws2_32)
endif()

if(MQ_BUILD_TESTS)
  enable_testing()
  # 测试代码使用断言表达验收条件，Release 测试也必须保留这些检查。
  if(MSVC)
    add_compile_options(/UNDEBUG)
  else()
    add_compile_options(-UNDEBUG)
  endif()
  add_executable(mq_core_tests test/unit/core_protocol_storage_test.cc)
  target_link_libraries(mq_core_tests PRIVATE mq_core mq_protocol)
  target_include_directories(mq_core_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_core_tests COMMAND mq_core_tests)

  add_executable(mq_protocol_fuzz_tests test/unit/protocol_fuzz_test.cc)
  target_link_libraries(mq_protocol_fuzz_tests PRIVATE mq_protocol)
  target_include_directories(mq_protocol_fuzz_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_protocol_fuzz_tests COMMAND mq_protocol_fuzz_tests)

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

  add_executable(mq_client_tests test/unit/client_test.cc)
  target_link_libraries(mq_client_tests PRIVATE mq_client mq_network mq_server)
  target_include_directories(mq_client_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_client_tests COMMAND mq_client_tests)

  add_executable(mq_replication_tests test/unit/replication_test.cc)
  target_link_libraries(mq_replication_tests PRIVATE mq_server)
  target_include_directories(mq_replication_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_replication_tests COMMAND mq_replication_tests)

  add_executable(mq_replication_client_tests test/integration/replication_client_test.cc)
  target_link_libraries(mq_replication_client_tests PRIVATE mq_server mq_network)
  target_include_directories(mq_replication_client_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
  add_test(NAME mq_replication_client_tests COMMAND mq_replication_client_tests)

  if(MQ_ENABLE_TLS)
    add_executable(mq_tls_integration_tests test/integration/tls_integration_test.cc)
    target_link_libraries(mq_tls_integration_tests PRIVATE mq_client mq_network)
    target_include_directories(mq_tls_integration_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    add_test(NAME mq_tls_integration_tests COMMAND mq_tls_integration_tests)
  endif()

  if(MQ_BUILD_TOOLS)
    add_test(NAME mq_admin_help COMMAND mq_admin --help)
    if(UNIX)
      add_test(NAME mq_bench_matrix_checker
               COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/tools/check_bench_matrix.sh
                       ${CMAKE_CURRENT_SOURCE_DIR}/tools/bench-results.fixture.csv 3)
    endif()
  endif()
endif()
