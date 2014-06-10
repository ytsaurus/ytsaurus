#pragma once

#include "public.h"
#include "parallel_awaiter.h"

#include <core/misc/error.h>

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
    typedef typename TParallelCollectorStorage<T>::TResultOrError TResultOrError;
    typedef typename TParallelCollectorStorage<T>::TResultsOrError TResultsOrError;
    typedef typename TParallelCollectorStorage<T>::TResults TResults;

    TParallelCollector();

    void Collect(TFuture<TResultOrError> future);

    TFuture<TResultsOrError> Complete();

private:
    typedef TParallelCollector<T> TThis;

    TParallelAwaiterPtr Awaiter_;
    TPromise<TResultsOrError> Promise_;
    std::atomic<bool> Completed_;
    std::atomic<int> CurrentIndex_;
    TParallelCollectorStorage<T> Results_;

    void OnResult(int index, TResultOrError result);
    void OnCompleted();
    void OnCanceled();

    bool TryLockCompleted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define PARALLEL_COLLECTOR_INL_H_
#include "parallel_collector-inl.h"
#undef PARALLEL_COLLECTOR_INL_H_
