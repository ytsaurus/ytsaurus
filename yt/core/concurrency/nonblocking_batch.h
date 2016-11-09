#pragma once

#include <yt/core/actions/future.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/misc/intrusive_ptr.h>

#include <queue>
#include <vector>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Nonblocking MPMC queue that supports batching.
/*!
 * TNonblockingBatch accepts 2 parameters:
 * - batchElements is maximum number of elements to be placed inside batch.
 * - batchDuration is a time period to create the batch.
 * If producer exceeds batchDuration the consumer receives awaited batch.
 * If there is no consumer thus the batch will be limited by batchElements.
 */
template <class T>
class TNonblockingBatch
    : public TRefCounted
{
public:
    using TBatch = std::vector<T>;

    TNonblockingBatch(size_t batchElements, TDuration batchDuration);
    ~TNonblockingBatch();

    template <class... U>
    void Enqueue(U&& ... u);

    TFuture<TBatch> DequeueBatch();

private:
    const size_t MaxBatchElements_;
    const TDuration BatchDuration_;

    TSpinLock SpinLock_;

    TBatch CurrentBatch_;
    bool TimeReady_ = false;
    std::queue<TBatch> Batches_;
    std::queue<TPromise<TBatch>> Promises_;
    TDelayedExecutorCookie BatchFlushCookie_;
    ui64 FlushGeneration_ = 0;

    void ResetTimer();
    void StartTimer(TGuard<TSpinLock>& guard);
    bool IsFlushNeeded(TGuard<TSpinLock>& guard) const;
    void CheckFlush(TGuard<TSpinLock>& guard);
    void CheckReturn(TGuard<TSpinLock>& guard);
    void OnBatchTimeout(ui64 gen);
};

template <class T>
using TNonblockingBatchPtr = TIntrusivePtr<TNonblockingBatch<T>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define NONBLOCKING_BATCH_INL_H_
#include "nonblocking_batch-inl.h"
#undef NONBLOCKING_BATCH_INL_H_
