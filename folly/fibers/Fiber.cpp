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

#include <folly/fibers/Fiber.h>

#include <algorithm>
#include <cstring>
#include <stdexcept>

#include <glog/logging.h>

#include <folly/Likely.h>
#include <folly/Portability.h>
#include <folly/fibers/FiberManagerInternal.h>
#include <folly/portability/SysSyscall.h>
#include <folly/portability/Unistd.h>

namespace folly {
namespace fibers {

namespace {
const uint64_t kMagic8Bytes = 0xfaceb00cfaceb00c;

std::thread::id localThreadId() {
  return std::this_thread::get_id();
}

/* Size of the region from p + nBytes down to the last non-magic value */
size_t nonMagicInBytes(unsigned char* stackLimit, size_t stackSize) {
  CHECK_EQ(reinterpret_cast<intptr_t>(stackLimit) % sizeof(uint64_t), 0u);
  CHECK_EQ(stackSize % sizeof(uint64_t), 0u);
  auto begin = reinterpret_cast<uint64_t*>(stackLimit);
  auto end = reinterpret_cast<uint64_t*>(stackLimit + stackSize);

  auto firstNonMagic = std::find_if(
      begin, end, [](uint64_t val) { return val != kMagic8Bytes; });

  return (end - firstNonMagic) * sizeof(uint64_t);
}

} // namespace

// 从 该fiber加入到  ready 链表中（ ready 或者 remote_ready), 很快将就被调度运行
void Fiber::resume() {
  DCHECK_EQ(state_, AWAITING);
  state_ = READY_TO_RUN;

  if (LIKELY(threadId_ == localThreadId())) {
    // 在同一个线程中， 则加入到readyFibers_ 链表中
    fiberManager_.readyFibers_.push_back(*this);
    fiberManager_.ensureLoopScheduled();
  } else {
    // 不在同一个线程，则 加入到 remote_ready中
    fiberManager_.remoteReadyInsert(this);
  }
}

Fiber::Fiber(FiberManager& fiberManager)
    : fiberManager_(fiberManager),
      fiberStackSize_(fiberManager_.options_.stackSize),
      fiberStackLimit_(fiberManager_.stackAllocator_.allocate(fiberStackSize_)),
      fiberImpl_([this] { fiberFunc(); }, fiberStackLimit_, fiberStackSize_) {
  fiberManager_.allFibers_.push_back(*this);

#ifdef FOLLY_SANITIZE_THREAD
  tsanCtx_ = __tsan_create_fiber(0);
#endif
}

void Fiber::init(bool recordStackUsed) {
// It is necessary to disable the logic for ASAN because we change
// the fiber's stack.
#ifndef FOLLY_SANITIZE_ADDRESS
  recordStackUsed_ = recordStackUsed;
  if (UNLIKELY(recordStackUsed_ && !stackFilledWithMagic_)) {
    CHECK_EQ(
        reinterpret_cast<intptr_t>(fiberStackLimit_) % sizeof(uint64_t), 0u);
    CHECK_EQ(fiberStackSize_ % sizeof(uint64_t), 0u);
    std::fill(
        reinterpret_cast<uint64_t*>(fiberStackLimit_),
        reinterpret_cast<uint64_t*>(fiberStackLimit_ + fiberStackSize_),
        kMagic8Bytes);

    stackFilledWithMagic_ = true;

    // newer versions of boost allocate context on fiber stack,
    // need to create a new one
    fiberImpl_ =
        FiberImpl([this] { fiberFunc(); }, fiberStackLimit_, fiberStackSize_);
  }
#else
  (void)recordStackUsed;
#endif
}

Fiber::~Fiber() {
#ifdef FOLLY_SANITIZE_ADDRESS
  if (asanFakeStack_ != nullptr) {
    fiberManager_.freeFakeStack(asanFakeStack_);
  }
  fiberManager_.unpoisonFiberStack(this);
#endif

#ifdef FOLLY_SANITIZE_THREAD
  __tsan_destroy_fiber(tsanCtx_);
#endif

  fiberManager_.stackAllocator_.deallocate(fiberStackLimit_, fiberStackSize_);
}

void Fiber::recordStackPosition() {
  // For ASAN builds, functions may run on fake stack.
  // So we cannot get meaningful stack position.
#ifndef FOLLY_SANITIZE_ADDRESS
  int stackDummy;
  auto currentPosition = static_cast<size_t>(
      fiberStackLimit_ + fiberStackSize_ -
      static_cast<unsigned char*>(static_cast<void*>(&stackDummy)));
  fiberManager_.recordStackPosition(currentPosition);
  VLOG(4) << "Stack usage: " << currentPosition;
#endif
}

[[noreturn]] void Fiber::fiberFunc() {
#ifdef FOLLY_SANITIZE_ADDRESS
  fiberManager_.registerFinishSwitchStackWithAsan(
      nullptr, &asanMainStackBase_, &asanMainStackSize_);
#endif

  while (true) {
    DCHECK_EQ(state_, NOT_STARTED);

    threadId_ = localThreadId();
    if (taskOptions_.logRunningTime) {
      prevDuration_ = std::chrono::microseconds(0);
      currStartTime_ = std::chrono::steady_clock::now();
    }
    state_ = RUNNING;

    try {
      if (resultFunc_) {
        DCHECK(finallyFunc_);
        DCHECK(!func_);

        resultFunc_();
      } else {
        DCHECK(func_);
        func_();
      }
    } catch (...) {
      fiberManager_.exceptionCallback_(
          std::current_exception(), "running Fiber func_/resultFunc_");
    }

    if (UNLIKELY(recordStackUsed_)) {
      auto newHighWatermark = fiberManager_.recordStackPosition(
          nonMagicInBytes(fiberStackLimit_, fiberStackSize_));
      VLOG(3) << "Max stack usage: " << newHighWatermark;
      CHECK_LT(newHighWatermark, fiberManager_.options_.stackSize - 64)
          << "Fiber stack overflow";
    }

    state_ = INVALID;
    // 从 fiber 切换到 main fiber 继续执行 main的内容
    fiberManager_.deactivateFiber(this);
  }
}

// Switch out of fiber context into the main context
void Fiber::preempt(State state) {
  auto preemptImpl = [&]() mutable {
    DCHECK_EQ(fiberManager_.activeFiber_, this);
    DCHECK_EQ(state_, RUNNING);
    DCHECK_NE(state, RUNNING);
    if (state != AWAITING_IMMEDIATE) {
      CHECK(fiberManager_.currentException_ == std::current_exception());
      CHECK_EQ(fiberManager_.numUncaughtExceptions_, uncaught_exceptions());
    }

    if (taskOptions_.logRunningTime) {
      auto now = std::chrono::steady_clock::now();
      prevDuration_ += now - currStartTime_;
      currStartTime_ = now;
    }
    state_ = state;   // 设置当前fiber 为新状态， yield-> Fiber::YIELDED. wait-> AWAITING

    recordStackPosition();

      //从 fiber 切换到 main fiber 继续执行 main的内容
    fiberManager_.deactivateFiber(this);  // 后续也是从这里恢复

    // Resumed from preemption
    DCHECK_EQ(fiberManager_.activeFiber_, this);
    DCHECK_EQ(state_, READY_TO_RUN);  // 事件继续，将状态改成阻塞等状态改成 READY_TO_RUN了
    if (taskOptions_.logRunningTime) {
      currStartTime_ = std::chrono::steady_clock::now();
    }
    state_ = RUNNING;   // fiber恢复后状态变成running
  };

  if (fiberManager_.preemptRunner_) {
    fiberManager_.preemptRunner_->run(std::ref(preemptImpl));
  } else {
    preemptImpl();
  }
}

Fiber::LocalData::~LocalData() {
  reset();
}

Fiber::LocalData::LocalData(const LocalData& other) : data_(nullptr) {
  *this = other;
}

Fiber::LocalData& Fiber::LocalData::operator=(const LocalData& other) {
  reset();
  if (!other.data_) {
    return *this;
  }

  vtable_ = other.vtable_;
  if (other.data_ == &other.buffer_) {
    data_ = vtable_.ctor_copy(&buffer_, other.data_);
  } else {
    data_ = vtable_.make_copy(other.data_);
  }

  return *this;
}

void Fiber::LocalData::reset() {
  if (!data_) {
    return;
  }

  if (data_ == &buffer_) {
    vtable_.dtor(data_);
  } else {
    vtable_.ruin(data_);
  }
  vtable_ = {};
  data_ = nullptr;
}

} // namespace fibers
} // namespace folly
