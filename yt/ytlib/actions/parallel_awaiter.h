#pragma once

#include "common.h"
#include "future.h"
#include "invoker_util.h"
#include "cancelable_context.h"

#include <ytlib/misc/error.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TParallelAwaiter
    : public TRefCounted
{
public:
    explicit TParallelAwaiter(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");
    explicit TParallelAwaiter(
        NProfiling::TProfiler* profiler = nullptr,
        const NYPath::TYPath& timerPath = "");

    template <class T>
    void Await(
        TFuture<T> result,
        TCallback<void(T)> onResult = TCallback<void(T)>());
    template <class T>
    void Await(
        TFuture<T> result,
        const Stroka& timerKey,
        TCallback<void(T)> onResult = TCallback<void(T)>());

    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        TCallback<void()> onResult = TCallback<void()>());
    //! Specialization of #Await for |T = void|.
    void Await(
        TFuture<void> result,
        const Stroka& timerKey,
        TCallback<void()> onResult = TCallback<void()>());

    TFuture<void> Complete(TClosure onComplete = TClosure());
    void Cancel();

    int GetRequestCount() const;
    int GetResponseCount() const;

    bool IsCompleted() const;
    TFuture<void> GetAsyncCompleted() const;

    bool IsCanceled() const;

private:
    TSpinLock SpinLock;

    bool Canceled;

    bool Completed;
    TPromise<void> CompletedPromise;
    TClosure OnComplete;

    bool Terminated;

    int RequestCount;
    int ResponseCount;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableInvoker;

    NProfiling::TProfiler* Profiler;
    NProfiling::TTimer Timer;

    bool TryAwait();

    void MaybeFireCompleted(const Stroka& key);
    void DoFireCompleted(TClosure onComplete);

    void Init(
        IInvokerPtr invoker,
        NProfiling::TProfiler* profiler,
        const NYPath::TYPath& timerPath);

    void Terminate();


    template <class T>
    void OnResult(
        const Stroka& key,
        TCallback<void(T)> onResult,
        T result);

    void OnResult(
        const Stroka& key,
        TCallback<void()> onResult);

};

typedef TIntrusivePtr<TParallelAwaiter> TParallelAwaiterPtr;

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

template <class T>
TParallelCollector<T>::TParallelCollector(
    NProfiling::TProfiler* profiler /* = nullptr */,
    const NYPath::TYPath& timerPath /* = "" */)
    : Awaiter(New<TParallelAwaiter>(profiler, timerPath))
    , Promise(NewPromise<TResultsOrError>())
    , Completed(false)
{ }

template <class T>
void TParallelCollector<T>::Collect(
    TFuture< TValueOrError<T> > future,
    const Stroka& timerKey /* = "" */)
{
    Awaiter->Await(
        future,
        timerKey,
        BIND(&TThis::OnResult, MakeStrong(this)));
}

template <class T>
TFuture< TValueOrError< std::vector<T> > > TParallelCollector<T>::Complete()
{
    Awaiter->Complete(
        BIND(&TThis::OnCompleted, MakeStrong(this)));
    return Promise;
}

template <class T>
void TParallelCollector<T>::OnResult(TValueOrError<T> result)
{
    if (result.IsOK()) {
        TGuard<TSpinLock> guard(SpinLock);
        Results.push_back(result.Value());
    } else {
        if (TryLockCompleted()) {
            Promise.Set(result);
        }
    }
}

template <class T>
void TParallelCollector<T>::OnCompleted()
{
    if (TryLockCompleted()) {
        Promise.Set(std::move(Results));
    }
}

template <class T>
bool TParallelCollector<T>::TryLockCompleted()
{
    return AtomicCas(&Completed, true, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PARALLEL_AWAITER_INL_H_
#include "parallel_awaiter-inl.h"
#undef PARALLEL_AWAITER_INL_H_
