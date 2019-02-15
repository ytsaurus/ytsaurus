#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple guard that executes a given function at the end of scope.

template <class TCallback>
class TFinallyGuard
{
public:
    template <class T>
    TFinallyGuard(T&& finally, bool noUncaughtExceptions)
        : Finally_(std::forward<T>(finally))
        , NoUncaughtExceptions_(noUncaughtExceptions)
    { }

    TFinallyGuard(TFinallyGuard&& guard)
        : Released_(guard.Released_)
        , Finally_(std::move(guard.Finally_))
        , NoUncaughtExceptions_(guard.NoUncaughtExceptions_)
    {
        guard.Release();
    }

    TFinallyGuard(const TFinallyGuard&) = delete;
    TFinallyGuard& operator=(const TFinallyGuard&) = delete;
    TFinallyGuard& operator=(TFinallyGuard&&) = delete;

    void Release()
    {
        Released_ = true;
    }

    ~TFinallyGuard()
    {
        if (!Released_ && !(NoUncaughtExceptions_ && std::uncaught_exceptions() > 0)) {
            Finally_();
        }
    }

private:
    bool Released_ = false;
    TCallback Finally_;
    bool NoUncaughtExceptions_;
};

template <class TCallback>
[[nodiscard]] TFinallyGuard<typename std::decay<TCallback>::type> Finally(TCallback&& callback, bool noUncaughtExceptions = false);

template <class TCallback>
TFinallyGuard<typename std::decay<TCallback>::type> Finally(TCallback&& callback, bool noUncaughtExceptions)
{
    return TFinallyGuard<typename std::decay<TCallback>::type>(std::forward<TCallback>(callback), noUncaughtExceptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
