#include "stdafx.h"
#include "async_stream_state.h"

#include <util/system/guard.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncStreamState::TAsyncStreamState()
    : IsOperationFinished(true)
    , IsActive_(true)
    , StaticError(MakePromise(TError()))
    , CurrentError()
{ }

void TAsyncStreamState::Cancel(const TError& error)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (!IsActive_) {
        return;
    }

    DoFail(error);
}

void TAsyncStreamState::Fail(const TError& error)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!IsActive_) {
        YASSERT(!StaticError.ToFuture().Get().IsOK());
        return;
    }

    DoFail(error);
}

void TAsyncStreamState::DoFail(const TError& error)
{
    YASSERT(!error.IsOK());
    IsActive_ = false;
    if (!CurrentError.IsNull()) {
        StaticError = CurrentError;
        CurrentError.Reset();
    } else {
        StaticError = TAsyncErrorPromise();
    }
    StaticError.Set(error);
}

void TAsyncStreamState::Close()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(IsActive_);

    IsActive_ = false;
    if (!CurrentError.IsNull()) {
        auto result = CurrentError;
        CurrentError.Reset();
        guard.Release();
        result.Set(TError());
    }
}

bool TAsyncStreamState::IsActive() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return IsActive_;
}

bool TAsyncStreamState::IsClosed() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return !IsActive_ && StaticError.ToFuture().Get().IsOK();
}

bool TAsyncStreamState::HasRunningOperation() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return !IsOperationFinished;
}

void TAsyncStreamState::Finish(const TError& error)
{
    if (error.IsOK()) {
        Close();
    } else {
        Fail(error);
    }
}

TError TAsyncStreamState::GetCurrentError()
{
    return StaticError.ToFuture().Get();
}

void TAsyncStreamState::StartOperation()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(IsOperationFinished);
    IsOperationFinished = false;
}

TAsyncError TAsyncStreamState::GetOperationError()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (IsOperationFinished || !IsActive_) {
        return StaticError;
    } else {
        YASSERT(!CurrentError.IsNull());
        CurrentError = TAsyncErrorPromise();
        return CurrentError;
    }
}

void TAsyncStreamState::FinishOperation(const TError& error)
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(!IsOperationFinished);
    IsOperationFinished = true;
    if (error.IsOK()) {
        if (IsActive_ && !CurrentError.IsNull()) {
            auto currentError = CurrentError;
            CurrentError.Reset();
            // Always release guard before setting future with 
            // unknown subscribers.
            guard.Release();

            currentError.Set(TError());
        }
    } else {
        DoFail(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
