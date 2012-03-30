#ifndef META_CHANGE_INL_H_
#error "Direct inclusion of this file is not allowed, include meta_change.h"
#endif

#include "composite_meta_state_detail.h"
#include "meta_state_manager.h"

#include <ytlib/misc/delayed_invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
TMetaChange<TResult>::TMetaChange(
    IMetaStateManager* metaStateManager,
    TChangeFunc func,
    const TSharedRef& changeData)
    : MetaStateManager(metaStateManager)
    , Func(func)
    , ChangeData(changeData)
    , Started(false)
    , Retriable(false)
{ }

template <class TResult>
typename TFuture<TResult>::TPtr TMetaChange<TResult>::Commit()
{
    YASSERT(!Started);
    Started = true;

    AsyncResult = New< TFuture<TResult> >();
    EpochContext = MetaStateManager->GetEpochContext();
    DoCommit();
    return AsyncResult;
}

template <class TResult>
void TMetaChange<TResult>::DoCommit()
{
    YASSERT(Started);
    MetaStateManager
        ->CommitChange(
            ChangeData,
            Bind(&TThis::ChangeFuncThunk, MakeStrong(this)))
        ->Subscribe(
            Bind(&TThis::OnCommitted, MakeStrong(this)));
}

template <class TResult>
typename TMetaChange<TResult>::TPtr TMetaChange<TResult>::SetRetriable(TDuration backoffTime)
{
    Retriable = true;
    BackoffTime = backoffTime;
    return this;
}

template <class TResult>
typename TMetaChange<TResult>::TPtr
TMetaChange<TResult>::OnSuccess(TCallback<void(TResult)> onSuccess)
{
    YASSERT(OnSuccess_.IsNull());
    OnSuccess_ = MoveRV(onSuccess);
    return this;
}

template <class TResult>
typename TMetaChange<TResult>::TPtr
TMetaChange<TResult>::OnError(TCallback<void()> onError)
{
    YASSERT(OnError_.IsNull());
    OnError_ = MoveRV(onError);
    return this;
}

template <class TResult>
void TMetaChange<TResult>::ChangeFuncThunk()
{
    Result = Func.Run();
}

template <class TResult>
void TMetaChange<TResult>::OnCommitted(ECommitResult result)
{
    if (result == ECommitResult::Committed) {
        if (!OnSuccess_.IsNull()) {
            OnSuccess_.Run(Result);
        }
    } else {
        if (!OnError_.IsNull()) {
            OnError_.Run();
        }
        if (Retriable) {
            TDelayedInvoker::Submit(
                Bind(&TThis::DoCommit, MakeStrong(this))
                .Via(MetaStateManager->GetStateInvoker(), EpochContext),
                BackoffTime);
        }
    }

    Result = TResult();
}

////////////////////////////////////////////////////////////////////////////////

template <class TTarget, class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    IMetaStateManager* metaStateManager,
    const TMessage& message,
    TResult (TTarget::* func)(const TMessage&),
    TTarget* target)
{
    YASSERT(metaStateManager);
    YASSERT(func);
    YASSERT(target);

    NProto::TMsgChangeHeader header;
    header.set_change_type(message.GetTypeName());

    auto changeData = SerializeChange(header, message);

    auto changeFunc = Bind(func, target, message);

    return New< TMetaChange<TResult> >(
        metaStateManager,
        changeFunc,
        TSharedRef(MoveRV(changeData)));
}

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    IMetaStateManager* metaStateManager,
    const TMessage& message,
    TCallback<TResult()> func)
{
    YASSERT(metaStateManager);
    YASSERT(!func.IsNull());

    NProto::TMsgChangeHeader header;
    header.set_change_type(message.GetTypeName());

    auto changeData = SerializeChange(header, message);

    return New< TMetaChange<TResult> >(
        metaStateManager,
        func,
        TSharedRef(MoveRV(changeData)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
