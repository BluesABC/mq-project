#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "mq/client/mq_client.h"
#include "mq/protocol/commands.h"

namespace {
constexpr const char* kBenchVersion = "bench_version=3";
struct Options {
  std::string mode, topic = "test", host = "127.0.0.1", group = "bench_group";
  std::uint16_t port = 9092;
  std::uint64_t messages = 1000000, size = 256;
  std::uint32_t connections = 1, batch = 100, partitions = 1;
};
bool Number(const std::string& value, std::uint64_t* result) { try { std::size_t used = 0; *result = std::stoull(value, &used); return used == value.size(); } catch (...) { return false; } }
bool Parse(int argc, char** argv, Options* options) {
  if (argc < 2) return false; options->mode = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string key = argv[i]; if (i + 1 >= argc) return false; const std::string value = argv[++i]; std::uint64_t number = 0;
    if (key == "--topic") options->topic = value; else if (key == "--host") options->host = value; else if (key == "--group") options->group = value;
    else if (key == "--port" && Number(value, &number) && number <= 65535) options->port = static_cast<std::uint16_t>(number);
    else if (key == "--messages" && Number(value, &number)) options->messages = number;
    else if (key == "--size" && Number(value, &number)) options->size = number;
    else if (key == "--connections" && Number(value, &number) && number > 0 && number <= 10000) options->connections = static_cast<std::uint32_t>(number);
    else if (key == "--batch" && Number(value, &number) && number > 0 && number <= 10000) options->batch = static_cast<std::uint32_t>(number);
    else if (key == "--partitions" && Number(value, &number) && number > 0 && number <= 1024) options->partitions = static_cast<std::uint32_t>(number);
    else return false;
  }
  return options->messages > 0 && options->size > 0;
}
bool TryCountMessage(std::atomic<std::uint64_t>* completed, std::uint64_t limit) {
  auto current = completed->load(std::memory_order_relaxed);
  while (current < limit &&
         !completed->compare_exchange_weak(current, current + 1,
                                            std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
  }
  return current < limit;
}
std::uint32_t EffectiveBatch(const Options& options) {
  constexpr std::uint64_t kBatchFixedBytes = 20;
  constexpr std::uint64_t kGeneratedKeyLimit = 64;
  const auto per_message = 2ULL + kGeneratedKeyLimit + 4ULL + options.size;
  if (per_message + kBatchFixedBytes > mq::protocol::kMaxPayloadBytes) return 0;
  const auto maximum = (mq::protocol::kMaxPayloadBytes - kBatchFixedBytes) / per_message;
  return static_cast<std::uint32_t>(std::max<std::uint64_t>(1, std::min<std::uint64_t>(options.batch, maximum)));
}
void Report(const Options& options, std::uint64_t count, std::chrono::steady_clock::time_point start, std::vector<double> latencies) {
  std::sort(latencies.begin(), latencies.end()); const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  auto percentile = [&](double p) { const auto index = static_cast<std::size_t>(p * latencies.size()); return latencies[index < latencies.size() ? index : latencies.size() - 1]; };
  std::cout << kBenchVersion << " mode=" << options.mode << " messages=" << count << " size=" << options.size << " connections=" << options.connections << " batch=" << options.batch << " partitions=" << options.partitions << " total_seconds=" << seconds << " TPS=" << count / seconds << " avg_latency_us=" << std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size() << " p50_us=" << percentile(.50) << " p99_us=" << percentile(.99) << " p999_us=" << percentile(.999) << '\n';
}
}

int main(int argc, char** argv) {
  if (argc == 2 && std::string(argv[1]) == "--version") {
    std::cout << kBenchVersion << '\n';
    return 0;
  }
  Options options; if (!Parse(argc, argv, &options) || (options.mode != "produce" && options.mode != "consume")) { std::cerr << "usage: mq_bench produce|consume --topic test --messages N --size bytes [--connections N] [--batch N] [--partitions N]\n"; return 2; }
  const auto effective_batch = EffectiveBatch(options);
  if (effective_batch == 0) { std::cerr << "message size exceeds protocol payload limit\n"; return 2; }
  options.batch = effective_batch;
  const auto start = std::chrono::steady_clock::now(); std::mutex latency_mutex; std::mutex error_mutex; std::string worker_error; std::vector<double> latencies; std::atomic<std::uint64_t> completed{0}; std::atomic<std::uint64_t> next_message{0}; std::atomic<bool> failed{false}; std::vector<std::uint64_t> producer_completed(options.connections, 0); std::vector<std::thread> workers;
  auto record_worker_error = [&](std::uint32_t connection, const std::exception& exception) {
    std::lock_guard lock(error_mutex);
    if (worker_error.empty()) {
      std::ostringstream message;
      message << "worker[" << connection << "]: " << exception.what();
      worker_error = message.str();
    }
    failed.store(true, std::memory_order_release);
  };
  try {
  if (options.mode == "produce") {
    mq::client::MqProducer setup; if (!setup.connect(options.host, options.port)) { std::cerr << "connect failed: " << setup.lastError() << '\n'; return 1; }
    setup.createTopic(options.topic, options.partitions); setup.close();
    for (std::uint32_t connection = 0; connection < options.connections; ++connection) workers.emplace_back([&, connection] {
      try {
        mq::client::MqProducer producer; producer.setProducerId(1000 + connection); if (!producer.connect(options.host, options.port)) { failed = true; return; }
        std::uint64_t done = 0;
        while (!failed.load()) {
          const auto begin_message = next_message.fetch_add(options.batch);
          if (begin_message >= options.messages) break;
          const auto current = static_cast<std::size_t>(std::min<std::uint64_t>(options.batch, options.messages - begin_message));
          std::vector<mq::client::ProducerMessage> batch; batch.reserve(current);
          for (std::size_t index = 0; index < current; ++index) batch.push_back({"bench-" + std::to_string(connection) + "-" + std::to_string(begin_message + index), std::string(options.size, 'x')});
          const auto begin = std::chrono::steady_clock::now();
          if (!producer.produceBatch(options.topic, batch)) { failed = true; break; }
          { std::lock_guard lock(latency_mutex); latencies.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - begin).count() / current); }
          done += current;
        }
        producer_completed[connection] = done;
        producer.close();
      } catch (const std::exception& exception) {
        record_worker_error(connection, exception);
      }
    });
  } else {
    for (std::uint32_t connection = 0; connection < options.connections; ++connection) workers.emplace_back([&, connection] {
      try {
        mq::client::MqConsumer consumer; if (!consumer.connect(options.host, options.port) || !consumer.subscribe(options.topic, options.group + "_" + std::to_string(connection), connection % options.partitions)) { failed = true; return; }
        auto idle_since = std::chrono::steady_clock::now();
        while (completed.load() < options.messages && !failed.load()) {
          const auto begin = std::chrono::steady_clock::now(); auto message = consumer.poll(1000);
          if (!message) {
            if (std::chrono::steady_clock::now() - idle_since > std::chrono::seconds(10)) { failed = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); continue;
          }
          idle_since = std::chrono::steady_clock::now();
          if (!consumer.commit(message->offset + 1)) { failed = true; break; }
          if (TryCountMessage(&completed, options.messages)) { std::lock_guard lock(latency_mutex); latencies.push_back(std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - begin).count()); }
        }
        consumer.close();
      } catch (const std::exception& exception) {
        record_worker_error(connection, exception);
      }
    });
  }
  } catch (const std::exception& exception) {
    std::lock_guard lock(error_mutex);
    worker_error = std::string("thread creation: ") + exception.what();
    failed.store(true, std::memory_order_release);
  }
  for (auto& worker : workers) worker.join();
  const auto total_completed = options.mode == "produce" ? std::accumulate(producer_completed.begin(), producer_completed.end(), std::uint64_t{0}) : completed.load();
  if (failed || total_completed != options.messages) { std::cerr << "benchmark failed: completed=" << total_completed << " expected=" << options.messages << " workers=" << options.connections << '\n'; if (!worker_error.empty()) std::cerr << worker_error << '\n'; for (std::size_t i = 0; i < producer_completed.size(); ++i) std::cerr << "worker[" << i << "]=" << producer_completed[i] << '\n'; return 1; }
  Report(options, total_completed, start, std::move(latencies)); return 0;
}
