#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNonBlockingQueue
    : public TRefCounted
{
public:
    void Enqueue(T&& value);
    TFuture<T> Dequeue();

private:
    TSpinLock SpinLock_;

    std::queue<T> ValueQueue_;
    std::queue<TPromise<T>> PromiseQueue_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define NON_BLOCKING_QUEUE_INL_H_
#include "non_blocking_queue-inl.h"
#undef NON_BLOCKING_QUEUE_INL_H_
