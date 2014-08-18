#ifndef NONBLOCKING_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include nonblocking_queue.h"
#endif
#undef NONBLOCKING_QUEUE_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template<class T> 
template<class TArg>
void TNonblockingQueue<T>::Enqueue(TArg&& value)
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (PromiseQueue_.empty()) {
        ValueQueue_.push(std::forward<T>(value));
    } else {
        auto promise = PromiseQueue_.front();
        PromiseQueue_.pop();
        guard.Release();
        promise.Set(std::forward<T>(value));
    }
}

template<class T>
TFuture<T> TNonblockingQueue<T>::Dequeue()
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (ValueQueue_.empty()) {
        auto promise = NewPromise<T>();
        PromiseQueue_.push(promise);
        return promise.ToFuture();
    } else {
        auto future = MakeFuture(ValueQueue_.front());
        ValueQueue_.pop();
        return future;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
