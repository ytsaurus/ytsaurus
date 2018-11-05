#pragma once
#ifndef NONBLOCKING_BATCH_INL_H_
#error "Direct inclusion of this file is not allowed, include nonblocking_batch.h"
// For the sake of sane code completion.
#include "nonblocking_batch.h"
#endif
#undef NONBLOCKING_BATCH_INL_H_

#include <yt/core/concurrency/delayed_executor.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TNonblockingBatch<T>::TNonblockingBatch(size_t batchElements, TDuration batchDuration)
    : MaxBatchElements_(batchElements)
    , BatchDuration_(batchDuration)
{ }

template <class T>
TNonblockingBatch<T>::~TNonblockingBatch()
{
    TGuard<TSpinLock> guard(SpinLock_);
    ResetTimer(guard);
}

template <class T>
template <class... U>
void TNonblockingBatch<T>::Enqueue(U&& ... u)
{
    TGuard<TSpinLock> guard(SpinLock_);
    CurrentBatch_.emplace_back(std::forward<U>(u)...);
    StartTimer(guard);
    CheckFlush(guard);
}

template <class T>
TFuture<typename TNonblockingBatch<T>::TBatch> TNonblockingBatch<T>::DequeueBatch()
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto promise = NewPromise<TBatch>();
    Promises_.push_back(promise);
    StartTimer(guard);
    CheckReturn(guard);
    guard.Release();
    return promise.ToFuture();
}

template <class T>
void TNonblockingBatch<T>::Drop()
{
    std::queue<TBatch> batches;
    std::deque<TPromise<TBatch>> promises;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        Batches_.swap(batches);
        Promises_.swap(promises);
        CurrentBatch_.clear();
        ResetTimer(guard);
    }
    for (auto&& promise : promises) {
        promise.Set(TBatch{});
    }
}

template <class T>
void TNonblockingBatch<T>::ResetTimer(TGuard<TSpinLock>& guard)
{
    if (TimerState_ == ETimerState::Started) {
        ++FlushGeneration_;
        TDelayedExecutor::CancelAndClear(BatchFlushCookie_);
    }
    TimerState_ = ETimerState::Initial;
}

template <class T>
void TNonblockingBatch<T>::StartTimer(TGuard<TSpinLock>& guard)
{
    if (TimerState_ == ETimerState::Initial && !Promises_.empty() && !CurrentBatch_.empty()) {
        TimerState_ = ETimerState::Started;
        BatchFlushCookie_ = TDelayedExecutor::Submit(
            BIND(&TNonblockingBatch::OnBatchTimeout, MakeWeak(this), FlushGeneration_),
            BatchDuration_);
    }
}

template <class T>
bool TNonblockingBatch<T>::IsFlushNeeded(TGuard<TSpinLock>& guard) const
{
    return CurrentBatch_.size() == MaxBatchElements_ || TimerState_ == ETimerState::Finished;
}

template <class T>
void TNonblockingBatch<T>::CheckFlush(TGuard<TSpinLock>& guard)
{
    if (!IsFlushNeeded(guard)) {
        return;
    }
    ResetTimer(guard);
    Batches_.push(std::move(CurrentBatch_));
    CurrentBatch_.clear();
    CheckReturn(guard);
}

template <class T>
void TNonblockingBatch<T>::CheckReturn(TGuard<TSpinLock>& guard)
{
    if (Promises_.empty() || Batches_.empty()) {
        return;
    }
    auto batch = std::move(Batches_.front());
    Batches_.pop();
    auto promise = std::move(Promises_.front());
    Promises_.pop_front();
    promise.Set(std::move(batch));
}

template <class T>
void TNonblockingBatch<T>::OnBatchTimeout(ui64 gen)
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (gen != FlushGeneration_) {
        // chunk had been prepared
        return;
    }
    TimerState_ = ETimerState::Finished;
    CheckFlush(guard);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
