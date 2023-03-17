/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/experimental/io/SimpleAsyncIO.h>

#include <folly/String.h>
#include <folly/experimental/coro/Baton.h>
#include <folly/experimental/io/AsyncIO.h>
#include <folly/experimental/io/IoUring.h>
#include <folly/portability/Sockets.h>

namespace folly {

#if __has_include(<libaio.h>)
static constexpr bool has_aio = true;
using aio_type = AsyncIO;
#else
static constexpr bool has_aio = false;
using aio_type = void;
#endif

#if __has_include(<liburing.h>)
static constexpr auto has_io_uring_rt = &IoUring::isAvailable;
using io_uring_type = IoUring;
#else
static constexpr auto has_io_uring_rt = +[] { return false; };
using io_uring_type = void;
#endif

template <typename AsyncIOType>
void SimpleAsyncIO::init() {
  asyncIO_ = std::make_unique<AsyncIOType>(maxRequests_, AsyncBase::POLLABLE);  // 使用 poll模式
  opsFreeList_.withWLock([this](auto& freeList) {
    for (size_t i = 0; i < maxRequests_; ++i) {
      freeList.push(std::make_unique<typename AsyncIOType::Op>());
    }
  });
}

template <>
void SimpleAsyncIO::init<void>() {}

SimpleAsyncIO::SimpleAsyncIO(Config cfg)
    : maxRequests_(cfg.maxRequests_),
      completionExecutor_(cfg.completionExecutor_),
      terminating_(false) {
  static bool has_io_uring = has_io_uring_rt();
  if (!has_aio && !has_io_uring) {
    LOG(FATAL) << "neither aio nor io_uring is available";
  }
  if (cfg.mode_ == AIO && !has_aio) {
    LOG(WARNING) << "aio requested but unavailable: falling back to io_uring";
    cfg.setMode(IOURING);
  }
  if (cfg.mode_ == IOURING && !has_io_uring) {
    LOG(WARNING) << "io_uring requested but unavailable: falling back to aio";
    cfg.setMode(AIO);
  }
  switch (cfg.mode_) {
    case AIO:
      init<aio_type>();
      break;
    case IOURING:
      init<io_uring_type>();
      break;
    default:
      // Should never happen...
      LOG(FATAL) << "unrecognized mode " << (int)cfg.mode_ << " requested";
      break;
  }

  // 使用 evenbase 对 pollfd进行事件驱动， 当io 完成时 调用 SimpleAsyncIO::handlerReady 处理完成的io
  if (cfg.evb_) {
    initHandler(cfg.evb_, NetworkSocket::fromFd(asyncIO_->pollFd()));
  } else {
    evb_ = std::make_unique<ScopedEventBaseThread>();
    initHandler(
        evb_->getEventBase(), NetworkSocket::fromFd(asyncIO_->pollFd()));
  }
  registerHandler(EventHandler::READ | EventHandler::PERSIST); // 注册pollfd 可读的handler
}

SimpleAsyncIO::~SimpleAsyncIO() {
  // stop accepting new IO.
  opsFreeList_.withWLock(
      [this](std::queue<std::unique_ptr<AsyncBaseOp>>& freeList) mutable {
        terminating_ = true;
        if (freeList.size() == maxRequests_) {
          drainedBaton_.post();
        }
      });

  drainedBaton_.wait();

  unregisterHandler();
}

// 当pollfd可读时调用该函数
void SimpleAsyncIO::handlerReady(uint16_t events) noexcept {
  if (events & EventHandler::READ) {
    // All the work (including putting op back on free list) happens in the
    // notificationCallback, so we can simply drop the ops returned from
    // pollCompleted. But we must still call it or ops never complete.
    while (asyncIO_->pollCompleted().size()) {
      ;
    }
  }
}

// 从op list中获取一个op
std::unique_ptr<AsyncBaseOp> SimpleAsyncIO::getOp() {
  std::unique_ptr<AsyncBaseOp> rc;
  opsFreeList_.withWLock(
      [this, &rc](std::queue<std::unique_ptr<AsyncBaseOp>>& freeList) {
        if (!freeList.empty() && !terminating_) {
          rc = std::move(freeList.front());
          freeList.pop();
          rc->reset();
        }
      });
  return rc;
}

void SimpleAsyncIO::putOp(std::unique_ptr<AsyncBaseOp>&& op) {
  opsFreeList_.withWLock(
      [this, op{std::move(op)}](
          std::queue<std::unique_ptr<AsyncBaseOp>>& freeList) mutable {
        freeList.push(std::move(op));
        if (terminating_ && freeList.size() == maxRequests_) {
          drainedBaton_.post();
        }
      });
}

// 设置 request 并提交
void SimpleAsyncIO::submitOp(
    Function<void(AsyncBaseOp*)> preparer, SimpleAsyncIOCompletor completor) {
  std::unique_ptr<AsyncBaseOp> opHolder = getOp(); // 从op list中获取一个op
  if (!opHolder) {
    completor(-EBUSY);
    return;
  }

  // Grab a raw pointer to the op before we create the completion lambda,
  // since we move the unique_ptr into the lambda and can no longer access
  // it.
  AsyncBaseOp* op = opHolder.get();

  preparer(op);  // 调用op 初始化函数

  // 设置io 完成的回调函数
  op->setNotificationCallback(
      [this, completor{std::move(completor)}, opHolder{std::move(opHolder)}](
          AsyncBaseOp* op_) mutable {
        CHECK(op_ == opHolder.get());
        int rc = op_->result(); // 检查op 结果

        completionExecutor_->add(
            [rc, completor{std::move(completor)}]() mutable { completor(rc); });  // 将完成的io任务放入到executor 执行 回调函数

        // NB: the moment we put the opHolder, the destructor might delete the
        // current instance. So do not access any member variables after this
        // point! Also, obviously, do not access op_.
        putOp(std::move(opHolder));  // 放回到free list 中
      });
  asyncIO_->submit(op);
}

void SimpleAsyncIO::pread(
    int fd,
    void* buf,
    size_t size,
    off_t start,
    SimpleAsyncIOCompletor completor) {
  submitOp(
      [=](AsyncBaseOp* op) { op->pread(fd, buf, size, start); },
      std::move(completor)); // 设置 request 并提交
}

void SimpleAsyncIO::pwrite(
    int fd,
    const void* buf,
    size_t size,
    off_t start,
    SimpleAsyncIOCompletor completor) {
  submitOp(
      [=](AsyncBaseOp* op) { op->pwrite(fd, buf, size, start); },
      std::move(completor));
}

#if FOLLY_HAS_COROUTINES
folly::coro::Task<int> SimpleAsyncIO::co_pwrite(
    int fd, const void* buf, size_t size, off_t start) {
  folly::coro::Baton done;
  int result;
  pwrite(fd, buf, size, start, [&done, &result](int rc) {
    result = rc;
    done.post();
  });
  co_await done;
  co_return result;
}

folly::coro::Task<int> SimpleAsyncIO::co_pread(
    int fd, void* buf, size_t size, off_t start) {
  folly::coro::Baton done;
  int result;
  pread(fd, buf, size, start, [&done, &result](int rc) {
    result = rc;
    done.post();
  });
  co_await done;
  co_return result;
}
#endif // FOLLY_HAS_COROUTINES

} // namespace folly
