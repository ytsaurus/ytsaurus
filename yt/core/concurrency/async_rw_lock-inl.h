#pragma once
#ifndef ASYNC_RW_LOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include async_rw_lock.h"
// For the sake of sane code completion
#include "async_rw_lock.h"
#endif
#undef ASYNC_RW_LOCK_INL_H_

#include <util/system/guard.h>

namespace NYT {
namespace NConcurrency {

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
