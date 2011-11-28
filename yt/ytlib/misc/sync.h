#pragma once

#include "common.h"
#include "async_stream_state.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////    

// TODO: write a couple of overloads manually, switch to Pump later
template <class TTarget>
void Sync(
    TTarget* target,
    TAsyncStreamState::TAsyncResult::TPtr (TTarget::*method)())
{
    auto result = (target->*method)()->Get();
    if (!result.IsOK) {
        // TODO: ToString()
        ythrow yexception() << result.ErrorMessage;
    }
}

template <class TTarget, class TArg1, class TArg1_>
void Sync(
    TTarget* target,
    TAsyncStreamState::TAsyncResult::TPtr (TTarget::*method)(TArg1),
    TArg1_&& arg1)
{
    auto result = (target->*method)(ForwardRV(arg1))->Get();
    if (!result.IsOK) {
        // TODO: ToString()
        ythrow yexception() << result.ErrorMessage;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
