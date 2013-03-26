#pragma once

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

    explicit TParallelCollector(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");

    explicit TParallelCollector(
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");

    void Collect(
        TFuture<TResultOrError> future,
        const Stroka& timerKey = "");

    TFuture<TResultsOrError> Complete();

private:
    typedef TParallelCollector<T> TThis;

    TParallelAwaiterPtr Awaiter;
    TPromise<TResultsOrError> Promise;
    TAtomic Completed;

    TParallelCollectorStorage<T> Results;

    void Init();

    void OnResult(TResultOrError result);
    void OnCompleted();

    bool TryLockCompleted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_COLLECTOR_INL_H_
#include "parallel_collector-inl.h"
#undef PARALLEL_COLLECTOR_INL_H_
