#ifndef ASYNC_RW_LOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include async_rw_lock.h"
#endif
#undef ASYNC_RW_LOCK_INL_H_

#include <util/system/guard.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncReaderWriterLock::AcquireReader()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (!HasActiveWriter_ && WriterPromiseQueue_.empty()) {
        ++ActiveReaderCount_;
        return VoidFuture;
    }

    auto promise = NewPromise<void>();
    ReaderPromiseQueue_.push(promise);
    return promise;
}

void TAsyncReaderWriterLock::ReleaseReader()
{
    TGuard<TSpinLock> guard(SpinLock_);

    YCHECK(ActiveReaderCount_ > 0);

    --ActiveReaderCount_;
    if (ActiveReaderCount_ == 0 && !WriterPromiseQueue_.empty()) {
        auto promise = WriterPromiseQueue_.front();
        WriterPromiseQueue_.pop();
        HasActiveWriter_ = true;
        guard.Release();
        promise.Set();
    }
}

TFuture<void> TAsyncReaderWriterLock::AcquireWriter()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (ActiveReaderCount_ == 0 && !HasActiveWriter_) {
        HasActiveWriter_ = true;
        return VoidFuture;
    }

    auto promise = NewPromise<void>();
    WriterPromiseQueue_.push(promise);
    return promise;
}

void TAsyncReaderWriterLock::ReleaseWriter()
{
    TGuard<TSpinLock> guard(SpinLock_);

    YCHECK(HasActiveWriter_);

    HasActiveWriter_ = false;
    if (WriterPromiseQueue_.empty()) {
        // Run all readers.
        if (!ReaderPromiseQueue_.empty()) {
            std::queue<TPromise<void>> readerPromiseQueue;
            readerPromiseQueue.swap(ReaderPromiseQueue_);
            ActiveReaderCount_ += readerPromiseQueue.size();
            guard.Release();
            while (!readerPromiseQueue.empty()) {
                readerPromiseQueue.front().Set();
                readerPromiseQueue.pop();
            }
        }
    } else {
        auto promise = WriterPromiseQueue_.front();
        WriterPromiseQueue_.pop();
        HasActiveWriter_ = true;
        guard.Release();
        promise.Set();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TOperationTraits>
TAsyncReaderWriterLockGuard<TOperationTraits>::~TAsyncReaderWriterLockGuard()
{
    Release();
}

template <typename TOperationTraits>
TFuture<typename TAsyncReaderWriterLockGuard<TOperationTraits>::TThisPtr>
    TAsyncReaderWriterLockGuard<TOperationTraits>::Acquire(TAsyncReaderWriterLock* lock)
{
    return TOperationTraits::Acquire(lock).Apply(BIND([=] () {
        auto guard = New<TThis>();
        guard->Lock_ = lock;
        return guard;
    }));
}

template <typename TOperationTraits>
void TAsyncReaderWriterLockGuard<TOperationTraits>::Release()
{
    if (Lock_) {
        TOperationTraits::Release(Lock_);
        Lock_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
