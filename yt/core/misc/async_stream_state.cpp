#include "stdafx.h"
#include "async_stream_state.h"

#include <util/system/guard.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncStreamState::TAsyncStreamState()
    : IsOperationFinished(true)
    , IsActive_(true)
    , StaticError(MakePromise(TError()))
{ }

void TAsyncStreamState::Cancel(const TError& error)
{
    {
        TGuard<TSpinLock> guard(SpinLock);

        if (!IsActive_) {
            return;
        }

        DoFail();
    }

    StaticError.Set(error);
}

void TAsyncStreamState::Fail(const TError& error)
{
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (!IsActive_) {
            return;
        }

        DoFail();
    }

    StaticError.Set(error);
}

void TAsyncStreamState::DoFail()
{
    IsActive_ = false;
    if (CurrentError) {
        StaticError = CurrentError;
        CurrentError.Reset();
    } else {
        StaticError = NewPromise<void>();
    }
}

void TAsyncStreamState::Close()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(IsActive_);

    IsActive_ = false;
    if (CurrentError) {
        auto result = CurrentError;
        CurrentError.Reset();
        guard.Release();
        result.Set(TError());
    }
}

bool TAsyncStreamState::IsActive() const
{
    // No guard for performance reasons; result can possibly be false positive.
    return IsActive_;
}

bool TAsyncStreamState::IsClosed() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return !IsActive_ && StaticError.Get().IsOK();
}

bool TAsyncStreamState::HasRunningOperation() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return !IsOperationFinished && IsActive_;
}

void TAsyncStreamState::Finish(const TError& error)
{
    if (error.IsOK()) {
        Close();
    } else {
        Fail(error);
    }
}

const TError& TAsyncStreamState::GetCurrentError()
{
    return StaticError.Get();
}

void TAsyncStreamState::StartOperation()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(IsOperationFinished);
    IsOperationFinished = false;
}

TFuture<void> TAsyncStreamState::GetOperationError()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (IsOperationFinished || !IsActive_) {
        return StaticError.ToFuture();
    } else {
        YASSERT(!CurrentError);
        CurrentError = NewPromise<void>();
        return CurrentError;
    }
}

void TAsyncStreamState::FinishOperation(const TError& error)
{
    TGuard<TSpinLock> guard(SpinLock);

    YASSERT(!IsOperationFinished);
    IsOperationFinished = true;
    if (error.IsOK()) {
        if (IsActive_ && CurrentError) {
            // Move constructor should eliminate redundant ref/unref.
            auto currentError(std::move(CurrentError));
            YASSERT(!CurrentError);
            // Always release guard before setting future with
            // unknown subscribers.
            guard.Release();

            currentError.Set(TError());
        }
    } else {
        DoFail();
        guard.Release();
        StaticError.Set(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
