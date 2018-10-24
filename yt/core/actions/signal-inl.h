#pragma once
#include <yt/core/misc/mpl.h>
#include <yt/core/misc/ref.h>

#ifndef SIGNAL_INL_H_
#error "Direct inclusion of this file is not allowed, include signal.h"
// For the sake of sane code completion
#include "signal.h"
#endif
#undef SIGNAL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::Subscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    Callbacks_.push_back(callback);
}

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
        if (*it == callback) {
            Callbacks_.erase(it);
            break;
        }
    }
}

template <class TResult, class... TArgs>
std::vector<TCallback<TResult(TArgs...)>> TCallbackList<TResult(TArgs...)>::ToVector() const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    return std::vector<TCallback>(Callbacks_.begin(), Callbacks_.end());
}

template <class TResult, class... TArgs>
int TCallbackList<TResult(TArgs...)>::Size() const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    return Callbacks_.size();
}

template <class TResult, class... TArgs>
bool TCallbackList<TResult(TArgs...)>::Empty() const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    return Callbacks_.empty();
}

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::Clear()
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    Callbacks_.clear();
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
void TCallbackList<TResult(TArgs...)>::Fire(TCallArgs&&... args) const
{
    TCallbackVector callbacks;
    {
        NConcurrency::TReaderGuard guard(SpinLock_);
        callbacks = Callbacks_;
    }

    for (const auto& callback : callbacks) {
        callback.Run(std::forward<TCallArgs>(args)...);
    }
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
void TCallbackList<TResult(TArgs...)>::FireAndClear(TCallArgs&&... args)
{
    TCallbackVector callbacks;
    {
        NConcurrency::TWriterGuard guard(SpinLock_);
        callbacks.swap(Callbacks_);
    }

    for (const auto& callback : callbacks) {
        callback.Run(std::forward<TCallArgs>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
void TSingleShotCallbackList<TResult(TArgs...)>::Subscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    if (Fired_) {
        guard.Release();
        callback.RunWithTuple(Args_);
        return;
    }
    Callbacks_.push_back(callback);
}

template <class TResult, class... TArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::TrySubscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    if (Fired_) {
        return false;
    }
    Callbacks_.push_back(callback);
    return true;
}

template <class TResult, class... TArgs>
void TSingleShotCallbackList<TResult(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
        if (*it == callback) {
            Callbacks_.erase(it);
            break;
        }
    }
}

template <class TResult, class... TArgs>
std::vector<TCallback<TResult(TArgs...)>> TSingleShotCallbackList<TResult(TArgs...)>::ToVector() const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    return std::vector<TCallback>(Callbacks_.begin(), Callbacks_.end());
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::Fire(TCallArgs&&... args)
{
    {
        NConcurrency::TWriterGuard guard(SpinLock_);
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

template <class TResult, class... TArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::IsFired() const
{
    NConcurrency::TReaderGuard guard(SpinLock_);
    return Fired_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
