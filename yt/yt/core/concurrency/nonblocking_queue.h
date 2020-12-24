#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/misc/ring_queue.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TNonblockingQueue
    : private TNonCopyable
{
public:
    // This template is required to enable perfect forwarding.
    template <class TArg>
    void Enqueue(TArg&& value);

    TFuture<T> Dequeue();

private:
    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);

    TRingQueue<TErrorOr<T>> ValueQueue_;
    TRingQueue<TPromise<T>> PromiseQueue_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define NONBLOCKING_QUEUE_INL_H_
#include "nonblocking_queue-inl.h"
#undef NONBLOCKING_QUEUE_INL_H_
