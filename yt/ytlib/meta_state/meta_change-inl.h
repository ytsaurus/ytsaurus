#ifndef META_CHANGE_INL_H_
#error "Direct inclusion of this file is not allowed, include meta_change.h"
#endif

#include "composite_meta_state_detail.h"

#include "../misc/assert.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
TMetaChange<TResult>::TMetaChange(
    TMetaStateManager* metaStateManager,
    TChangeFunc* func,
    const TSharedRef& changeData,
    ECommitMode mode)
    : MetaStateManager(metaStateManager)
    , ChangeFunc(func)
    , ChangeData(changeData)
    , CommitMode(mode)
    , Started(false)
{ }

template <class TResult>
typename TFuture<TResult>::TPtr TMetaChange<TResult>::Commit()
{
    YASSERT(!Started);
    Started = true;

    AsyncResult = New< TFuture<TResult> >();

    MetaStateManager
        ->CommitChangeSync(
            FromMethod(&TThis::ChangeFuncThunk, TPtr(this)),
            ChangeData,
            CommitMode)
         ->Subscribe(
            FromMethod(&TThis::OnCommitted, TPtr(this)));

    return AsyncResult;
}

template <class TResult>
typename TMetaChange<TResult>::TPtr
TMetaChange<TResult>::OnSuccess(typename IParamAction<TResult>::TPtr onSuccess)
{
    YASSERT(~OnSuccess_ == NULL);
    OnSuccess_ = onSuccess;
    return this;
}

template <class TResult>
typename TMetaChange<TResult>::TPtr
TMetaChange<TResult>::OnError(IAction::TPtr onError)
{
    YASSERT(~OnError_ == NULL);
    OnError_ = onError;
    return this;
}

template <class TResult>
void TMetaChange<TResult>::ChangeFuncThunk()
{
    Result = ChangeFunc->Do();
}

template <class TResult>
void TMetaChange<TResult>::OnCommitted(ECommitResult result)
{
    if (result == ECommitResult::Committed) {
        if (~OnSuccess_ != NULL) {
            OnSuccess_->Do(Result);
        }
    } else {
        if (~OnError_ != NULL) {
            OnError_->Do();
        }
    }

    Result = TResult();
}

////////////////////////////////////////////////////////////////////////////////

template <class TTarget, class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    TMetaStateManager* metaStateManager,
    const TMessage& message,
    TResult (TTarget::* func)(const TMessage&),
    TTarget* target,
    ECommitMode mode)
{
    YASSERT(metaStateManager != NULL);
    YASSERT(func != NULL);
    YASSERT(target != NULL);

    NProto::TMsgChangeHeader header;
    header.SetChangeType(message.GetTypeName());

    auto changeData = SerializeChange(header, message);

    auto changeFunc = FromMethod(func, target, message);

    return New< TMetaChange<TResult> >(
        metaStateManager,
        ~changeFunc,
        TSharedRef(MoveRV(changeData)),
        mode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
