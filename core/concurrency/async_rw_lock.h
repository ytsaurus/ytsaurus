#pragma once

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/scheduler.h>

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TAsyncReaderWriterLock
    : public TNonCopyable
{
public:
    TFuture<void> AcquireReader();
    void ReleaseReader();

    TFuture<void> AcquireWriter();
    void ReleaseWriter();

private:
    int ActiveReaderCount_ = 0;
    bool HasActiveWriter_ = false;

    std::vector<TPromise<void>> ReaderPromiseQueue_;
    std::queue<TPromise<void>> WriterPromiseQueue_;

    TSpinLock SpinLock_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOperationTraits>
class TAsyncReaderWriterLockGuard
    : public TRefCounted
{
private:
    using TThis = TAsyncReaderWriterLockGuard;
    using TThisPtr = TIntrusivePtr<TThis>;

public:
    ~TAsyncReaderWriterLockGuard();

    static TFuture<TThisPtr> Acquire(TAsyncReaderWriterLock* lock);

    void Release();

private:
    TAsyncReaderWriterLock* Lock_ = nullptr;

};

class TAsyncLockReaderTraits
{
public:
    static TFuture<void> Acquire(TAsyncReaderWriterLock* lock)
    {
        return lock->AcquireReader();
    }

    static void Release(TAsyncReaderWriterLock* lock)
    {
        lock->ReleaseReader();
    }
};

class TAsyncLockWriterTraits
{
public:
    static TFuture<void> Acquire(TAsyncReaderWriterLock* lock)
    {
        return lock->AcquireWriter();
    }

    static void Release(TAsyncReaderWriterLock* lock)
    {
        lock->ReleaseWriter();
    }
};

using TAsyncLockReaderGuard = TAsyncReaderWriterLockGuard<TAsyncLockReaderTraits>;
using TAsyncLockWriterGuard = TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define ASYNC_RW_LOCK_INL_H_
#include "async_rw_lock-inl.h"
#undef ASYNC_RW_LOCK_INL_H_
