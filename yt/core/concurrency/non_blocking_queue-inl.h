#ifndef NON_BLOCKING_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include non_blocking_queue-inl.h"
#endif
#undef NON_BLOCKING_QUEUE_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template<class T>
void TNonBlockingQueue<T>::Enqueue(T&& value)
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (PromiseQueue_.empty()) {
        ValueQueue_.push(std::forward<T>(value));
    } else {
        auto promise = PromiseQueue_.front();
        promise.Set(std::forward<T>(value));
        PromiseQueue_.pop();
    }
}

template<class T>
TFuture<T> TNonBlockingQueue<T>::Dequeue()
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
