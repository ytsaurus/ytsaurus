#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple guard that executes a given function at the end of scope.
class TFinallyGuard
    : public TNonCopyable
{
public:
    explicit TFinallyGuard(std::function<void()> finally)
        : Finally_(std::move(finally))
    { }

    ~TFinallyGuard()
    {
        Finally_();
    }

private:
    const std::function<void()> Finally_;

};

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
class TStaticFinallyGuard
{
public:
    template <class T>
    explicit TStaticFinallyGuard(T&& finally)
        : Finally_(std::forward<T>(finally))
    { }

    ~TStaticFinallyGuard()
    {
        Finally_();
    }

private:
    TCallback Finally_;

};

template <class TCallback>
TStaticFinallyGuard<typename std::decay<TCallback>::type> Finally(TCallback&& callback)
{
    return TStaticFinallyGuard<typename std::decay<TCallback>::type>(std::forward<TCallback>(callback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
