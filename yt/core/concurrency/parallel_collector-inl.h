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

    void Store(int index, const TErrorOr<T>& valueOrError)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Values_.size() <= index) {
            Values_.resize(index + 1);
        }
        Values_[index] = valueOrError.Value();
    }

    void SetPromise(TPromise<TResults>& promise)
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

    void Store(int /*index*/, const TErrorOr<void>& /*valueOrError*/)
    { }

    void SetPromise(TPromise<TResults>& promise)
    {
        promise.Set(TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParallelCollector<T>::TParallelCollector()
    : Awaiter_(New<TParallelAwaiter>(GetSyncInvoker()))
    , Promise_(NewPromise<TResults>())
    , Completed_(false)
    , CurrentIndex_(0)
{ }

template <class T>
void TParallelCollector<T>::Collect(TFuture<T> future)
{
    int index = CurrentIndex_++;
    Awaiter_->Await(
        future,
        BIND(&TThis::OnResult, MakeStrong(this), index));
}

template <class T>
auto TParallelCollector<T>::Complete() -> TFuture<TResults>
{
    auto awaiterFuture = Awaiter_->Complete();
    awaiterFuture.Subscribe(BIND(&TThis::OnCompleted, MakeStrong(this)));
    return Promise_;
}

template <class T>
void TParallelCollector<T>::OnResult(int index, const TErrorOr<T>& result)
{
    if (result.IsOK()) {
        Storage_.Store(index, result);
    } else {
        if (TryLockCompleted()) {
            // NB: Do not replace TError(result) with result unless you understand
            // the consequences! Consult ignat@ or babenko@.
            Promise_.Set(TError(result));
        }
    }
}

template <class T>
void TParallelCollector<T>::OnCompleted(const TError& error)
{
    if (TryLockCompleted()) {
        if (error.IsOK()) {
            Storage_.SetPromise(Promise_);
        } else {
            Promise_.Set(error);
        }
    }
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
