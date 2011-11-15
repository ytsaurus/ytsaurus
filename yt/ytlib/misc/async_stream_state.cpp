#include "stdafx.h"
#include "async_stream_state.h"

#include <util/system/guard.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncStreamState::TResult::TResult(
    bool isOk,
    const Stroka& errorMessage)
    : IsOK(isOk)
    , ErrorMessage(errorMessage)
{ }

////////////////////////////////////////////////////////////////////////////////

TAsyncStreamState::TAsyncStreamState()
    : IsOperationFinished(true)
    , IsActive_(true)
    , StaticResult(New<TAsyncResult>())
    , CurrentResult(NULL)
{
    StaticResult->Set(TResult());
}

void TAsyncStreamState::Cancel(const Stroka& errorMessage)
{
    TGuard<TSpinLock> guard(SpinLock);

    if (!IsActive_) {
        return;
    }

    DoFail(errorMessage);
}

void TAsyncStreamState::Fail(const Stroka& errorMessage)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (!IsActive_) {
        YASSERT(!StaticResult->Get().IsOK);
        return;
    }

    DoFail(errorMessage);
}

void TAsyncStreamState::DoFail(const Stroka& errorMessage)
{
    IsActive_ = false;
    if (~CurrentResult != NULL) {
        StaticResult = CurrentResult;
        CurrentResult.Reset();
    } else {
        StaticResult = New<TAsyncResult>();
    }
    StaticResult->Set(TResult(false, errorMessage));
}

void TAsyncStreamState::Close()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(IsActive_);

    IsActive_ = false;
    if (~CurrentResult != NULL) {
        auto result = CurrentResult;
        CurrentResult.Reset();

        result->Set(TResult());
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
    return !IsActive_ && StaticResult->Get().IsOK;
}

bool TAsyncStreamState::HasRunningOperation() const
{
    TGuard<TSpinLock> guard(SpinLock);
    return !IsOperationFinished;
}

void TAsyncStreamState::Finish(TResult result)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (result.IsOK) {
        Close();
    } else {
        Fail(result.ErrorMessage);
    }
}

auto TAsyncStreamState::GetCurrentResult() -> TResult
{
    return StaticResult->Get();
}

void TAsyncStreamState::StartOperation()
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(IsOperationFinished);
    IsOperationFinished = false;
}

auto TAsyncStreamState::GetOperationResult() -> TAsyncResult::TPtr
{
    TGuard<TSpinLock> guard(SpinLock);
    if (IsOperationFinished || !IsActive_) {
        return StaticResult;
    } else {
        YASSERT(~CurrentResult == NULL);
        CurrentResult = New<TAsyncResult>();
        return CurrentResult;
    }
}

void TAsyncStreamState::FinishOperation(TResult result)
{
    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(!IsOperationFinished);
    IsOperationFinished = true;
    if (result.IsOK) {
        if (IsActive_ && ~CurrentResult != NULL) {
            auto result = CurrentResult;
            CurrentResult.Reset();

            result->Set(TResult());
        }
    } else {
        DoFail(result.ErrorMessage);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
