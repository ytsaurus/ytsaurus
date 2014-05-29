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
        TGuard<TSpinLock> guard(SpinLock_);
        if (static_cast<int>(Values_.size()) <= index) {
            Values_.resize(index + 1);
        }
        Values_[index] = std::move(valueOrError.Value());
    }

    void SetPromise(TPromise<TResultsOrError> promise)
    {
        promise.Set(std::move(Values_));
    }

private:
    TSpinLock SpinLock_;
    TResults Values_;

};

////////////////////////////////////////////////////////////////////////////////

template <>
class TParallelCollectorStorage<void>
{
public:
    typedef void TResults;
    typedef TError TResultOrError;
    typedef TError TResultsOrError;

    void Store(int /*index*/, const TErrorOr<void>& /*valueOrError*/)
    { }

    void SetPromise(TPromise<TResultsOrError> promise)
    {
        promise.Set(TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParallelCollector<T>::TParallelCollector()
    : Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
    , Promise_(NewPromise<TResultsOrError>())
    , CurrentIndex_(0)
{
    // XXX(babenko): VS2013 compat
    Completed_ = false;
}

template <class T>
void TParallelCollector<T>::Collect(TFuture<TResultOrError> future)
{
    int index = CurrentIndex_++;
    Awaiter_->Await(
        future,
        BIND(&TThis::OnResult, MakeStrong(this), index));
}

template <class T>
TFuture<typename TParallelCollector<T>::TResultsOrError> TParallelCollector<T>::Complete()
{
    auto awaiterFuture = Awaiter_->Complete();
    awaiterFuture.Subscribe(BIND(&TThis::OnCompleted, MakeStrong(this)));
    awaiterFuture.OnCanceled(BIND(&TThis::OnCanceled, MakeStrong(this)));
    return Promise_;
}

template <class T>
void TParallelCollector<T>::OnResult(int index, TResultOrError result)
{
    if (result.IsOK()) {
        Results_.Store(index, result);
    } else {
        if (TryLockCompleted()) {
            // NB: Do not replace TError(result) with result unless you understand
            // the consequences! Consult ignat@ or babenko@.
            Promise_.Set(TError(result));
        }
    }
}

template <class T>
void TParallelCollector<T>::OnCompleted()
{
    if (TryLockCompleted()) {
        Results_.SetPromise(Promise_);
    }
}

template <class T>
void TParallelCollector<T>::OnCanceled()
{
    Promise_.Cancel();
}

template <class T>
bool TParallelCollector<T>::TryLockCompleted()
{
    bool expected = false;
    return Completed_.compare_exchange_strong(expected, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
