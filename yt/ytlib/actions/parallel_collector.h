#pragma once

#include "parallel_awaiter.h"

#include <ytlib/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParallelCollector
    : public TRefCounted
{
public:
    typedef std::vector<T> TResults;
    typedef TValueOrError<TResults> TResultsOrError;

    explicit TParallelCollector(
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");

    void Collect(
        TFuture< TValueOrError<T> > future,
        const Stroka& timerKey = "");

    TFuture<TResultsOrError> Complete();

private:
    typedef TParallelCollector<T> TThis;

    TParallelAwaiterPtr Awaiter;
    TPromise<TResultsOrError> Promise;
    TAtomic Completed;

    TSpinLock SpinLock;
    std::vector<T> Results;

    void OnResult(TValueOrError<T> result);
    void OnCompleted();

    bool TryLockCompleted();

};

////////////////////////////////////////////////////////////////////////////////

template <>
class TParallelCollector<void>
    : public TRefCounted
{
public:
    explicit TParallelCollector(
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");

    void Collect(
        TFuture<TError> future,
        const Stroka& timerKey = "");
    
    void Collect(
        TFuture<TValueOrError<void>> future,
        const Stroka& timerKey = "");

    TFuture<TError> Complete();

private:
    typedef TParallelCollector<void> TThis;

    TParallelAwaiterPtr Awaiter;
    TPromise<TError> Promise;
    TAtomic Completed;

    TSpinLock SpinLock;

    void OnResult(TError result);
    void OnCompleted();

    bool TryLockCompleted();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_COLLECTOR_INL_H_
#include "parallel_collector-inl.h"
#undef PARALLEL_COLLECTOR_INL_H_
