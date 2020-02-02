#pragma once

#include <yt/core/misc/mpl.h>
#include <yt/core/misc/ref.h>

#ifndef SIGNAL_INL_H_
#error "Direct inclusion of this file is not allowed, include signal.h"
// For the sake of sane code completion.
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
void TCallbackList<TResult(TArgs...)>::Fire(const TArgs&... args) const
{
    TCallbackVector callbacks;
    {
        NConcurrency::TReaderGuard guard(SpinLock_);
        callbacks = Callbacks_;
    }

    for (const auto& callback : callbacks) {
        callback.Run(args...);
    }
}

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::FireAndClear(const TArgs&... args)
{
    TCallbackVector callbacks;
    {
        NConcurrency::TWriterGuard guard(SpinLock_);
        callbacks.swap(Callbacks_);
    }

    for (const auto& callback : callbacks) {
        callback.Run(args...);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
void TSingleShotCallbackList<TResult(TArgs...)>::Subscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    if (Fired_.load(std::memory_order_acquire)) {
        guard.Release();
        std::apply(callback, Args_);
        return;
    }
    Callbacks_.push_back(callback);
}

template <class TResult, class... TArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::TrySubscribe(const TCallback& callback)
{
    NConcurrency::TWriterGuard guard(SpinLock_);
    if (Fired_.load(std::memory_order_acquire)) {
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
    TCallbackVector callbacks;
    {
        NConcurrency::TWriterGuard guard(SpinLock_);
        if (Fired_.load(std::memory_order_acquire)) {
            return false;
        }
        Args_ = std::make_tuple(std::forward<TCallArgs>(args)...);
        callbacks.swap(Callbacks_);
        Fired_.store(true, std::memory_order_release);
    }

    for (const auto& callback : callbacks) {
        std::apply(callback, Args_);
    }

    return true;
}

template <class TResult, class... TArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::IsFired() const
{
    return Fired_.load(std::memory_order_acquire);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
