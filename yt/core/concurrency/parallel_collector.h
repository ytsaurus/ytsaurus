#pragma once

#include "public.h"
#include "parallel_awaiter.h"

#include <core/actions/future.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParallelCollectorStorage;

template <class T>
class TParallelCollector
    : public TRefCounted
{
public:
    typedef typename TParallelCollectorStorage<T>::TResults TResults;

    TParallelCollector();

    void Collect(TFuture<T> future);

    TFuture<TResults> Complete();

private:
    typedef TParallelCollector<T> TThis;

    TParallelAwaiterPtr Awaiter_;
    TPromise<TResults> Promise_;
    std::atomic<bool> Completed_;
    std::atomic<int> CurrentIndex_;
    TParallelCollectorStorage<T> Storage_;

    void OnResult(int index, const TErrorOr<T>& result);
    void OnCompleted(const TError& error);

    bool TryLockCompleted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define PARALLEL_COLLECTOR_INL_H_
#include "parallel_collector-inl.h"
#undef PARALLEL_COLLECTOR_INL_H_
