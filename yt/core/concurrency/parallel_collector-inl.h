#ifndef PARALLEL_COLLECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include parallel_collector.h"
#endif
#undef PARALLEL_COLLECTOR_INL_H_

#include <core/actions/invoker_util.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TParallelCollectorStorage
{
public:
    typedef std::vector<T> TResults;
    typedef TErrorOr<T> TResultOrError;
    typedef TErrorOr<TResults> TResultsOrError;

    void Store(int index, const TErrorOr<T>& valueOrError)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (static_cast<int>(Values.size()) <= index) {
            Values.resize(index + 1);
        }
        Values[index] = std::move(valueOrError.GetValue());
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

    void Store(int index, const TErrorOr<void>& valueOrError)
    {
        UNUSED(index);
        UNUSED(valueOrError);
    }

    void SetPromise(TPromise<TResultsOrError> promise)
    {
        promise.Set(TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParallelCollector<T>::TParallelCollector()
    : Awaiter(New<TParallelAwaiter>(GetSyncInvoker()))
    , Promise(NewPromise<TResultsOrError>())
    , Completed(false)
    , CurrentIndex(0)
{ }

template <class T>
void TParallelCollector<T>::Collect(TFuture<TResultOrError> future)
{
    int index = AtomicIncrement(CurrentIndex) - 1;
    Awaiter->Await(
        future,
        BIND(&TThis::OnResult, MakeStrong(this), index));
}

template <class T>
TFuture<typename TParallelCollector<T>::TResultsOrError> TParallelCollector<T>::Complete()
{
    Awaiter->Complete(
        BIND(&TThis::OnCompleted, MakeStrong(this)));
    return Promise;
}

template <class T>
void TParallelCollector<T>::OnResult(int index, TResultOrError result)
{
    if (result.IsOK()) {
        Results.Store(index, result);
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

} // namespace NConcurrency
} // namespace NYT
