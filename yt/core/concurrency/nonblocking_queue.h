#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNonblockingQueue
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

#define NONBLOCKING_QUEUE_INL_H_
#include "nonblocking_queue-inl.h"
#undef NONBLOCKING_QUEUE_INL_H_
