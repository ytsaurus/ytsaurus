#include <core/misc/ref.h>
#include <core/misc/mpl.h>

#ifndef SIGNAL_INL_H_
#error "Direct inclusion of this file is not allowed, include signal.h"
#endif
#undef SIGNAL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
void TCallbackList<void(TArgs...)>::Subscribe(const TCallback& callback)
{
    TGuard<TSpinLock> guard(SpinLock_);
    Callbacks_.push_back(callback);
}

template <class... TArgs>
void TCallbackList<void(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    TGuard<TSpinLock> guard(SpinLock_);
    for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
        if (*it == callback) {
            Callbacks_.erase(it);
            break;
        }
    }
}

template <class... TArgs>
void TCallbackList<void(TArgs...)>::Clear()
{
    TGuard<TSpinLock> guard(SpinLock_);
    Callbacks_.clear();
}

template <class... TArgs>
template <class... TCallArgs>
void TCallbackList<void(TArgs...)>::Fire(TCallArgs&&... args) const
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (Callbacks_.empty())
        return;

    auto callbacks = Callbacks_;
    guard.Release();

    for (const auto& callback : callbacks) {
        callback.Run(std::forward<TCallArgs>(args)...);
    }
}

template <class... TArgs>
template <class... TCallArgs>
void TCallbackList<void(TArgs...)>::FireAndClear(TCallArgs&&... args)
{
    SmallVector<TCallback, 4> callbacks;
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Callbacks_.empty())
            return;
        callbacks.swap(Callbacks_);
    }

    for (const auto& callback : callbacks) {
        callback.Run(std::forward<TCallArgs>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
void TSingleShotCallbackList<void(TArgs...)>::Subscribe(const TCallback& callback)
{
    TGuard<TSpinLock> guard(SpinLock_);
    if (Fired_) {
        guard.Release();
        callback.RunWithTuple(Args_);
        return;
    }
    Callbacks_.push_back(callback);
}

template <class... TArgs>
void TSingleShotCallbackList<void(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    TGuard<TSpinLock> guard(SpinLock_);
    for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
        if (*it == callback) {
            Callbacks_.erase(it);
            break;
        }
    }
}

template <class... TArgs>
template <class... TCallArgs>
bool TSingleShotCallbackList<void(TArgs...)>::Fire(TCallArgs&&... args)
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (Fired_) {
            return false;
        }
        Fired_ = true;
        Args_ = std::make_tuple(std::forward<TCallArgs>(args)...);
    }

    for (const auto& callback : Callbacks_) {
        callback.RunWithTuple(Args_);
    }

    Callbacks_.clear();

    return true;
}

template <class... TArgs>
bool TSingleShotCallbackList<void(TArgs...)>::IsFired() const
{
    TGuard<TSpinLock> guard(SpinLock_);
    return Fired_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
