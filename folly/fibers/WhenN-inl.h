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

#include <folly/Optional.h>
#include <folly/fibers/FiberManagerInternal.h>
#include <folly/fibers/ForEach.h>

namespace folly {
namespace fibers {

// 这个函数是 folly 库中的 collectN 函数，它的作用是等待一个由多个 Future 组成的数组中的
// 前 n 个 Future 完成，并返回它们的结果。
template <class InputIterator>
typename std::enable_if<
    !std::is_same<
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>,
        void>::value,
    std::vector<std::pair<
        size_t,
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>>>>::type
collectN(InputIterator first, InputIterator last, size_t n) {
  typedef invoke_result_t<
      typename std::iterator_traits<InputIterator>::value_type>
      Result;
  assert(n > 0);
  assert(std::distance(first, last) >= 0);
  assert(n <= static_cast<size_t>(std::distance(first, last)));

  struct Context {
    std::vector<std::pair<size_t, Result>> results; // 用于存储已经完成的 Future 的结果
    size_t tasksTodo;       // 表示还有多少个 Future 没有完成
    std::exception_ptr e;
    folly::Optional<Promise<void>> promise;  // 用于在所有n个 Future 完成后通知调用者

    Context(size_t tasksTodo_) : tasksTodo(tasksTodo_) {
      this->results.reserve(tasksTodo_);
    }
  };
  auto context = std::make_shared<Context>(n);

  await_async([first, last, context](Promise<void> promise) mutable {
    context->promise = std::move(promise);
    for (size_t i = 0; first != last; ++i, ++first) { // 于每个 Future，都会创建一个新的 Task，并将其加入到 FiberManager 的任务队列中
      addTask([i, context, f = std::move(*first)]() {
        try {
          auto result = f();
          if (context->tasksTodo == 0) {
            return;
          }
          context->results.emplace_back(i, std::move(result));
        } catch (...) {
          if (context->tasksTodo == 0) {
            return;
          }
          context->e = std::current_exception();
        }
        if (--context->tasksTodo == 0) { // 表示n哥 Future 都已经完成，此时调用 promise->setValue() 函数通知调用者
          context->promise->setValue();
        }
      });
    }
  });

  if (context->e != std::exception_ptr()) {
    std::rethrow_exception(context->e);
  }

  return std::move(context->results);
}

template <class InputIterator>
typename std::enable_if<
    std::is_same<
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>,
        void>::value,
    std::vector<size_t>>::type
collectN(InputIterator first, InputIterator last, size_t n) {
  assert(n > 0);
  assert(std::distance(first, last) >= 0);
  assert(n <= static_cast<size_t>(std::distance(first, last)));

  struct Context {
    std::vector<size_t> taskIndices;  // 而是返回已经完成的 Future 在数组中的下标
    std::exception_ptr e;
    size_t tasksTodo;
    folly::Optional<Promise<void>> promise;

    Context(size_t tasksTodo_) : tasksTodo(tasksTodo_) {
      this->taskIndices.reserve(tasksTodo_);
    }
  };
  auto context = std::make_shared<Context>(n);

  await_async([first, last, context](Promise<void> promise) mutable {
    context->promise = std::move(promise);
    for (size_t i = 0; first != last; ++i, ++first) {
      addTask([i, context, f = std::move(*first)]() {
        try {
          f();
          if (context->tasksTodo == 0) {
            return;
          }
          context->taskIndices.push_back(i);
        } catch (...) {
          if (context->tasksTodo == 0) {
            return;
          }
          context->e = std::current_exception();
        }
        if (--context->tasksTodo == 0) {
          context->promise->setValue();
        }
      });
    }
  });

  if (context->e != std::exception_ptr()) {
    std::rethrow_exception(context->e);
  }

  return context->taskIndices;  // 返回已经完成的 Future 在数组中的下标
}

template <class InputIterator>
typename std::vector<
    typename std::enable_if<
        !std::is_same<
            invoke_result_t<
                typename std::iterator_traits<InputIterator>::value_type>,
            void>::value,
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>>::
        type> inline collectAll(InputIterator first, InputIterator last) {
  typedef invoke_result_t<
      typename std::iterator_traits<InputIterator>::value_type>
      Result;
  size_t n = size_t(std::distance(first, last));
  std::vector<Result> results;
  std::vector<size_t> order(n);
  results.reserve(n);

  forEach(first, last, [&results, &order](size_t id, Result result) {
    order[id] = results.size();
    results.emplace_back(std::move(result));
  });
  assert(results.size() == n);

  std::vector<Result> orderedResults;
  orderedResults.reserve(n);

  for (size_t i = 0; i < n; ++i) {
    orderedResults.emplace_back(std::move(results[order[i]]));
  }

  return orderedResults;
}

template <class InputIterator>
typename std::enable_if<
    std::is_same<
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>,
        void>::value,
    void>::type inline collectAll(InputIterator first, InputIterator last) {
  forEach(first, last, [](size_t /* id */) {});
}

template <class InputIterator>
typename std::enable_if<
    !std::is_same<
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>,
        void>::value,
    typename std::pair<
        size_t,
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>>>::
    type inline collectAny(InputIterator first, InputIterator last) {
  auto result = collectN(first, last, 1);
  assert(result.size() == 1);
  return std::move(result[0]);
}

template <class InputIterator>
typename std::enable_if<
    std::is_same<
        invoke_result_t<
            typename std::iterator_traits<InputIterator>::value_type>,
        void>::value,
    size_t>::type inline collectAny(InputIterator first, InputIterator last) {
  auto result = collectN(first, last, 1);
  assert(result.size() == 1);
  return std::move(result[0]);
}
} // namespace fibers
} // namespace folly
