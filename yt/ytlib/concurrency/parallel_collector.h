#pragma once

#include "public.h"
#include "parallel_awaiter.h"

#include <ytlib/misc/error.h>

namespace NYT {

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

    TParallelAwaiterPtr Awaiter;
    TPromise<TResultsOrError> Promise;
    TAtomic Completed;
    TAtomic CurrentIndex;
    TParallelCollectorStorage<T> Results;

    void OnResult(int index, TResultOrError result);
    void OnCompleted();

    bool TryLockCompleted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_COLLECTOR_INL_H_
#include "parallel_collector-inl.h"
#undef PARALLEL_COLLECTOR_INL_H_
