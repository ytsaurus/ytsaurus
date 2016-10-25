#pragma once
#ifndef NONBLOCKING_BATCH_INL_H_
#error "Direct inclusion of this file is not allowed, include nonblocking_batch.h"
#endif
#undef NONBLOCKING_BATCH_INL_H_

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
    ResetTimer();
}

template <class T>
template <class... U>
void TNonblockingBatch<T>::Enqueue(U&& ... u)
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (CurrentBatch_.empty()) {
        StartTimer(guard);
    }
    CurrentBatch_.emplace_back(std::forward<U>(u)...);
    CheckFlush(guard);
}

template <class T>
TFuture<typename TNonblockingBatch<T>::TBatch> TNonblockingBatch<T>::DequeueBatch()
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto promise = NewPromise<TBatch>();
    Promises_.push(promise);
    CheckReturn(guard);
    guard.Release();
    return promise.ToFuture();
}

template <class T>
void TNonblockingBatch<T>::ResetTimer()
{
    TDelayedExecutor::CancelAndClear(BatchFlushCookie_);
}

template <class T>
void TNonblockingBatch<T>::StartTimer(TGuard<TSpinLock>& guard)
{
    BatchFlushCookie_ = TDelayedExecutor::Submit(
        BIND(&TNonblockingBatch::OnBatchTimeout, MakeWeak(this), ++FlushGeneration_),
        BatchDuration_);
}

template <class T>
bool TNonblockingBatch<T>::IsFlushNeeded(TGuard<TSpinLock>& guard) const
{
    return CurrentBatch_.size() == MaxBatchElements_ || TimeReady_ && !Promises_.empty();
}

template <class T>
void TNonblockingBatch<T>::CheckFlush(TGuard<TSpinLock>& guard)
{
    if (!IsFlushNeeded(guard)) {
        return;
    }
    ResetTimer();
    Batches_.push(std::move(CurrentBatch_));
    CurrentBatch_.clear();
    TimeReady_ = false;
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
    Promises_.pop();
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
    TimeReady_ = true;
    CheckFlush(guard);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
