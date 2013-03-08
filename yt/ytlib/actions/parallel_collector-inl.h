#ifndef PARALLEL_COLLECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_collector.h"
#endif
#undef PARALLEL_COLLECTOR_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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
            // NB: Do not replace TError(result) with result unless you understand
            // the consequences! Consult ignat@ or babenko@.
            Promise.Set(TError(result));
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

inline
TParallelCollector<void>::TParallelCollector(
    NProfiling::TProfiler* profiler /* = nullptr */,
    const NYPath::TYPath& timerPath /* = "" */)
    : Awaiter(New<TParallelAwaiter>(profiler, timerPath))
    , Promise(NewPromise<TError>())
    , Completed(false)
{ }

inline
void TParallelCollector<void>::Collect(
    TFuture<TError> future,
    const Stroka& timerKey /* = "" */)
{
    Awaiter->Await(
        future,
        timerKey,
        BIND(&TThis::OnResult, MakeStrong(this)));
}

inline
void TParallelCollector<void>::Collect(
    TFuture<TValueOrError<void>> future,
    const Stroka& timerKey /* = "" */)
{
    Awaiter->Await(
        future.Apply(BIND([](TValueOrError<void> error) -> TError {
            return error; 
        })),
        timerKey,
        BIND(&TThis::OnResult, MakeStrong(this)));
}

inline
TFuture<TError> TParallelCollector<void>::Complete()
{
    Awaiter->Complete(
        BIND(&TThis::OnCompleted, MakeStrong(this)));
    return Promise;
}

inline
void TParallelCollector<void>::OnResult(TError result)
{
    if (!result.IsOK()) {
        if (TryLockCompleted()) {
            Promise.Set(result);
        }
    }
}

inline
void TParallelCollector<void>::OnCompleted()
{
    if (TryLockCompleted()) {
        Promise.Set(TError());
    }
}

inline
bool TParallelCollector<void>::TryLockCompleted()
{
    return AtomicCas(&Completed, true, false);
}

////////////////////////////////////////////////////////////////////////////////
} // namespace NYT
