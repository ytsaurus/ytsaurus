#ifndef META_CHANGE_INL_H_
#error "Direct inclusion of this file is not allowed, include meta_change.h"
#endif

#include "composite_meta_state_detail.h"
#include "meta_state_manager.h"

#include <ytlib/misc/delayed_invoker.h>

#include <util/random/random.h>

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
    , Promise(Null)
{ }

template <class TResult>
TFuture<TResult> TMetaChange<TResult>::Commit()
{
    YASSERT(!Started);
    Started = true;

    Promise = NewPromise<TResult>();
    EpochContext = MetaStateManager->GetEpochContext();
    DoCommit();
    return Promise;
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
    OnSuccess_ = onSuccess;
    return this;
}

template <class TResult>
typename TMetaChange<TResult>::TPtr
TMetaChange<TResult>::OnError(const TClosure& onError)
{
    YASSERT(OnError_.IsNull());
    OnError_ = onError;
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

template <class TMessage>
NProto::TChangeHeader GetMetaChangeHeader(const TMessage& message)
{
    NProto::TChangeHeader header;
    header.set_change_type(message.GetTypeName());
    header.set_timestamp(TInstant::Now().GetValue());
    header.set_random_seed(RandomNumber<ui32>());
    return header;
}

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TCallback<TResult()>& func)
{
    YASSERT(metaStateManager);
    YASSERT(!func.IsNull());

    auto changeData = SerializeChange(GetMetaChangeHeader(message), message);

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

    auto changeData = SerializeChange(GetMetaChangeHeader(message), messageData);

    return New< TMetaChange<TResult> >(
        metaStateManager,
        func,
        TSharedRef(MoveRV(changeData)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
