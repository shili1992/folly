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

#include <folly/io/async/EventHandler.h>

#include <cassert>

#include <folly/String.h>
#include <folly/io/async/EventBase.h>

namespace folly {

// 设置了运行所在的 eventbase 和 fd
EventHandler::EventHandler(EventBase* eventBase, NetworkSocket fd) {
  event_.eb_event_set(fd.data, 0, &EventHandler::libeventCallback, this);
  if (eventBase != nullptr) {
    setEventBase(eventBase);
  } else {
    // Callers must set the EventBase and fd before using this timeout.
    // Set event_->ev_base to nullptr to ensure that this happens.
    // (otherwise libevent will initialize it to the "default" event_base)
    event_.eb_ev_base(nullptr);
    eventBase_ = nullptr;
  }
}

EventHandler::~EventHandler() {
  unregisterHandler();
}

// 注册fd上events事件， 设置了回调函数
bool EventHandler::registerImpl(uint16_t events, bool internal) {
  assert(event_.eb_ev_base() != nullptr);

  // We have to unregister the event before we can change the event flags
  if (isHandlerRegistered()) {
    // If the new events are the same are the same as the already registered
    // flags, we don't have to do anything.  Just return.
    auto flags = folly::event_ref_flags(event_.getEvent());
    if (events == event_.eb_ev_events() &&
        static_cast<bool>(flags & EVLIST_INTERNAL) == internal) {
      return true;
    }

    event_.eb_event_del();
  }

  // Update the event flags
  // Unfortunately, event_set() resets the event_base, so we have to remember
  // it before hand, then pass it back into event_base_set() afterwards
  auto* evb = event_.eb_ev_base();
  // 设置libevent中evnet fd 事件 回调函数等, 注意这里设置进去的为this，
  // 当IO完成时， 统一调用libeventCallback -> handle_ready
  event_.eb_event_set(
      event_.eb_ev_fd(), short(events), &EventHandler::libeventCallback, this);
  event_.eb_event_base_set(evb);  // 设置 事件 从属的event_base

  // Set EVLIST_INTERNAL if this is an internal event
  if (internal) {
    folly::event_ref_flags(event_.getEvent()) |= EVLIST_INTERNAL;
  }

  // Add the event.
  //
  // Although libevent allows events to wait on both I/O and a timeout,
  // we intentionally don't allow an EventHandler to also use a timeout.
  // Callers must maintain a separate AsyncTimeout object if they want a
  // timeout.
  //
  // Otherwise, it is difficult to handle persistent events properly.  (The I/O
  // event and timeout may both fire together the same time around the event
  // loop.  Normally we would want to inform the caller of the I/O event first,
  // then the timeout.  However, it is difficult to do this properly since the
  // I/O callback could delete the EventHandler.)  Additionally, if a caller
  // uses the same struct event for both I/O and timeout, and they just want to
  // reschedule the timeout, libevent currently makes an epoll_ctl() call even
  // if the I/O event flags haven't changed.  Using a separate event struct is
  // therefore slightly more efficient in this case (although it does take up
  // more space).
  // 添加事件,
  if (event_.eb_event_add(nullptr) < 0) {
    LOG(ERROR) << "EventBase: failed to register event handler for fd "
               << event_.eb_ev_fd() << ": " << errnoStr(errno);
    // Call event_del() to make sure the event is completely uninstalled
    event_.eb_event_del();
    return false;
  }

  return true;
}

void EventHandler::unregisterHandler() {
  if (isHandlerRegistered()) {
    event_.eb_event_del();
  }
}

void EventHandler::attachEventBase(EventBase* eventBase) {
  // attachEventBase() may only be called on detached handlers
  assert(event_.eb_ev_base() == nullptr);
  assert(!isHandlerRegistered());
  // This must be invoked from the EventBase's thread
  eventBase->dcheckIsInEventBaseThread();

  setEventBase(eventBase);
}

void EventHandler::detachEventBase() {
  ensureNotRegistered(__func__);
  event_.eb_ev_base(nullptr);
}

void EventHandler::changeHandlerFD(NetworkSocket fd) {
  ensureNotRegistered(__func__);
  // event_set() resets event_base.ev_base, so manually restore it afterwards
  auto* evb = event_.eb_ev_base();
  event_.eb_event_set(fd.data, 0, &EventHandler::libeventCallback, this);
  event_.eb_ev_base(
      evb); // don't use event_base_set(), since evb may be nullptr
}

void EventHandler::initHandler(EventBase* eventBase, NetworkSocket fd) {
  ensureNotRegistered(__func__);
  event_.eb_event_set(fd.data, 0, &EventHandler::libeventCallback, this);
  setEventBase(eventBase);
}

void EventHandler::ensureNotRegistered(const char* fn) {
  // Neither the EventBase nor file descriptor may be changed while the
  // handler is registered.  Treat it as a programmer bug and abort the program
  // if this requirement is violated.
  if (isHandlerRegistered()) {
    LOG(ERROR) << fn << " called on registered handler; aborting";
    abort();
  }
}

// 注册的回调函数， 调用handlerReady
void EventHandler::libeventCallback(libevent_fd_t fd, short events, void* arg) {
  auto handler = reinterpret_cast<EventHandler*>(arg);
  assert(fd == handler->event_.eb_ev_fd());
  (void)fd; // prevent unused variable warnings

  auto observer = handler->eventBase_->getExecutionObserver();
  if (observer) {
    observer->starting(reinterpret_cast<uintptr_t>(handler));
  }

  // this can't possibly fire if handler->eventBase_ is nullptr
  handler->eventBase_->bumpHandlingTime();

  handler->handlerReady(uint16_t(events));  // 调用 接口的handlerReady 函数

  if (observer) {
    observer->stopped(reinterpret_cast<uintptr_t>(handler));
  }
}

void EventHandler::setEventBase(EventBase* eventBase) {
  event_.eb_event_base_set(eventBase);
  eventBase_ = eventBase;
}

bool EventHandler::isPending() const {
  if (folly::event_ref_flags(event_.getEvent()) & EVLIST_ACTIVE) {
    if (event_.eb_ev_res() & EV_READ) {
      return true;
    }
  }
  return false;
}

} // namespace folly
