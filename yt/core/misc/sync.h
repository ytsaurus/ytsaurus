#pragma once

#include "common.h"
#include "error.h"

#include <core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: write a couple of overloads manually, switch to Pump later
template <class TTarget, class TTargetConvertible>
void Sync(
    TTarget* target,
    TFuture<void>(TTargetConvertible::*method)())
{
    auto result = (target->*method)().Get();
    if (!result.IsOK()) {
        THROW_ERROR result;
    }
}

template <class TTarget, class TTargetConvertible>
void Sync(
    TTarget* target,
    TFuture<void>&(TTargetConvertible::*method)())
{
    auto& result = (target->*method)().Get();
    if (!result.IsOK()) {
        THROW_ERROR result;
    }
}

template <class TTarget, class TTargetConvertible,  class TArg1, class TArg1_>
void Sync(
    TTarget* target,
    TFuture<void>(TTargetConvertible::*method)(TArg1),
    TArg1_&& arg1)
{
    auto result = (target->*method)(std::forward<TArg1>(arg1)).Get();
    if (!result.IsOK()) {
        THROW_ERROR result;
    }
}

template <class TTarget, class TTargetConvertible, class TArg1, class TArg1_, class TArg2, class TArg2_>
void Sync(
    TTarget* target,
    TFuture<void>(TTargetConvertible::*method)(TArg1, TArg2),
    TArg1_&& arg1,
    TArg2_&& arg2)
{
    auto result = (target->*method)(
        std::forward<TArg1>(arg1),
        std::forward<TArg2>(arg2)).Get();

    if (!result.IsOK()) {
        THROW_ERROR result;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
