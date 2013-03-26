#ifndef PARALLEL_COLLECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_collector.h"
#endif
#undef PARALLEL_COLLECTOR_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParallelCollectorStorage
{
public:
    typedef std::vector<T> TResults;
    typedef TValueOrError<T> TResultOrError;
    typedef TValueOrError<TResults> TResultsOrError;

    void Store(const TValueOrError<T>& valueOrError)
    {
        TGuard<TSpinLock> guard(SpinLock);
        Values.push_back(valueOrError.Value());
    }

    void SetPromise(TPromise<TResultsOrError> promise)
    {
        promise.Set(std::move(Values));
    }

private:
    TSpinLock SpinLock;
    TResults Values;

};

////////////////////////////////////////////////////////////////////////////////

template <>
class TParallelCollectorStorage<void>
{
public:
    typedef void TResults;
    typedef TError TResultOrError;
    typedef TError TResultsOrError;

    void Store(const TValueOrError<void>& valueOrError)
    {
        UNUSED(valueOrError);
    }

    void SetPromise(TPromise<TResultsOrError> promise)
    {
        promise.Set(TError());
    }
};

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
    TFuture<TResultOrError> future,
    const Stroka& timerKey /* = "" */)
{
    Awaiter->Await(
        future,
        timerKey,
        BIND(&TThis::OnResult, MakeStrong(this)));
}

template <class T>
TFuture<typename TParallelCollector<T>::TResultsOrError> TParallelCollector<T>::Complete()
{
    Awaiter->Complete(
        BIND(&TThis::OnCompleted, MakeStrong(this)));
    return Promise;
}

template <class T>
void TParallelCollector<T>::OnResult(TResultOrError result)
{
    if (result.IsOK()) {
        Results.Store(result);
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
        Results.SetPromise(Promise);
    }
}

template <class T>
bool TParallelCollector<T>::TryLockCompleted()
{
    return AtomicCas(&Completed, true, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
