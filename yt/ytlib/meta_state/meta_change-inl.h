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
    const IMetaStateManagerPtr& metaStateManager,
    const TChangeFunc& func,
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
            BIND(&TThis::ChangeFuncThunk, MakeStrong(this)))
        .Subscribe(
            BIND(&TThis::OnCommitted, MakeStrong(this)));
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
TMetaChange<TResult>::OnSuccess(const TCallback<void(TResult)>& onSuccess)
{
    YASSERT(OnSuccess_.IsNull());
    OnSuccess_ = MoveRV(onSuccess);
    return this;
}

template <class TResult>
typename TMetaChange<TResult>::TPtr
TMetaChange<TResult>::OnError(const TCallback<void()>& onError)
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
                BIND(&TThis::DoCommit, MakeStrong(this))
                .Via(MetaStateManager->GetStateInvoker(), EpochContext),
                BackoffTime);
        }
    }

    Result = TResult();
}

////////////////////////////////////////////////////////////////////////////////

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TCallback<TResult()>& func)
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

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    const IMetaStateManagerPtr& metaStateManager,
    TRef messageData,
    const TMessage& message,
    const TCallback<TResult()>& func)
{
    YASSERT(metaStateManager);
    YASSERT(!func.IsNull());

    NProto::TMsgChangeHeader header;
    header.set_change_type(message.GetTypeName());

    auto changeData = SerializeChange(header, messageData);

    return New< TMetaChange<TResult> >(
        metaStateManager,
        func,
        TSharedRef(MoveRV(changeData)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
