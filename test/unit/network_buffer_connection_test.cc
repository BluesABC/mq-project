#include <atomic>
#include <cassert>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "mq/core/memory_pool.h"
#include "mq/network/buffer.h"
#include "mq/network/tcp_connection.h"

namespace {

void EnforcesMemoryPoolOwnershipAndCapacity() {
  mq::core::MemoryPool pool(32);
  assert(pool.Allocate(16, alignof(std::max_align_t)) != nullptr);
  assert(pool.Allocate(17, alignof(char)) == nullptr);
  std::atomic<void*> foreign_result{reinterpret_cast<void*>(1)};
  std::thread foreign_thread([&] { foreign_result.store(pool.Allocate(1, alignof(char))); });
  foreign_thread.join();
  assert(foreign_result.load() == nullptr);
  mq::network::Buffer buffer(&pool, 16);
  assert(buffer.Append("abcdef"));
  buffer.Consume(2);
  assert(buffer.Append("ghijklmnop"));
  assert(buffer.Readable() == "cdefghijklmnop");
  assert(!buffer.Append("qrs"));
}

void ConnectionKeepsBuffersOnOwnerLoop() {
  mq::network::EventLoop loop(32);
  assert(loop.Start());
  std::shared_ptr<mq::network::TcpConnection> connection;
  std::shared_ptr<mq::core::MemoryPool> pool;
  std::string received;
  auto setup_done = std::make_shared<std::promise<void>>();
  const auto setup_ready = setup_done->get_future();
  assert(loop.QueueInLoop([&] {
    pool = std::make_shared<mq::core::MemoryPool>(256);
    connection = std::make_shared<mq::network::TcpConnection>(1, &loop, pool.get(), 64, 64);
    connection->SetReadCallback([&](std::shared_ptr<mq::network::TcpConnection>, std::string_view bytes) {
      received.assign(bytes);
      return bytes.size();
    });
    assert(connection->OnReadable("request"));
    setup_done->set_value();
  }));
  setup_ready.wait();
  assert(received == "request");
  assert(connection->Send("response"));
  auto write_done = std::make_shared<std::promise<void>>();
  const auto write_ready = write_done->get_future();
  assert(loop.QueueInLoop([&] {
    assert(connection->Writable() == "response");
    connection->ConsumeWritten(8);
    assert(connection->Writable().empty());
    write_done->set_value();
  }));
  write_ready.wait();
  loop.Stop();
}

}  // namespace

int main() {
  EnforcesMemoryPoolOwnershipAndCapacity();
  ConnectionKeepsBuffersOnOwnerLoop();
  return 0;
}
