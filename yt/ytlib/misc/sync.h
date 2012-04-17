#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////    

// TODO: write a couple of overloads manually, switch to Pump later
template <class TTarget>
void Sync(
    TTarget* target,
    TIntrusivePtr< TFuture<TError> > (TTarget::*method)())
{
    auto result = (target->*method)().Get();
    if (!result.IsOK()) {
        ythrow yexception() << result.ToString();
    }
}

template <class TTarget, class TArg1, class TArg1_>
void Sync(
    TTarget* target,
    TIntrusivePtr< TFuture<TError> > (TTarget::*method)(TArg1),
    TArg1_&& arg1)
{
    auto result = (target->*method)(ForwardRV<TArg1>(arg1)).Get();
    if (!result.IsOK()) {
        ythrow yexception() << result.ToString();
    }
}

template <class TTarget, class TArg1, class TArg1_, class TArg2, class TArg2_>
void Sync(
    TTarget* target,
    TIntrusivePtr< TFuture<TError> > (TTarget::*method)(TArg1, TArg2),
    TArg1_&& arg1,
    TArg2_&& arg2)
{
    auto result = (target->*method)(
        ForwardRV<TArg1>(arg1), 
        ForwardRV<TArg2>(arg2)).Get();

    if (!result.IsOK()) {
        ythrow yexception() << result.ToString();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
