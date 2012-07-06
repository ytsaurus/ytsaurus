#ifndef MUTATION_INL_H_
#error "Direct inclusion of this file is not allowed, include mutation.h"
#endif

#include "meta_state_manager.h"

#include <ytlib/misc/delayed_invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
TMutation<TResult>::TMutation(
    const IMetaStateManagerPtr& metaStateManager,
    const TMutationAction& mutationAction,
    const Stroka& mutationType,
    const TSharedRef& mutationData)
    : MetaStateManager(metaStateManager)
    , MutationAction(mutationAction)
    , MutationType(mutationType)
    , MutationData(mutationData)
    , Started(false)
    , Retriable(false)
    , Promise(Null)
{ }

template <class TResult>
TFuture<TResult> TMutation<TResult>::Commit()
{
    YASSERT(!Started);
    Started = true;

    Promise = NewPromise<TResult>();
    EpochContext = MetaStateManager->GetEpochContext();
    DoCommit();
    return Promise;
}

template <class TResult>
void TMutation<TResult>::DoCommit()
{
    YASSERT(Started);
    MetaStateManager
        ->CommitMutation(
            MutationType,
            MutationData,
            BIND(&TThis::MutationActionThunk, MakeStrong(this)))
        .Subscribe(
            BIND(&TThis::OnCommitted, MakeStrong(this)));
}

template <class TResult>
typename TMutation<TResult>::TPtr TMutation<TResult>::SetRetriable(TDuration backoffTime)
{
    Retriable = true;
    BackoffTime = backoffTime;
    return this;
}

template <class TResult>
typename TMutation<TResult>::TPtr
TMutation<TResult>::OnSuccess(const TCallback<void(TResult)>& onSuccess)
{
    YASSERT(OnSuccess_.IsNull());
    OnSuccess_ = onSuccess;
    return this;
}

template <class TResult>
typename TMutation<TResult>::TPtr
TMutation<TResult>::OnError(const TClosure& onError)
{
    YASSERT(OnError_.IsNull());
    OnError_ = onError;
    return this;
}

template <class TResult>
void TMutation<TResult>::MutationActionThunk()
{
    Result = MutationAction.Run();
}

template <class TResult>
void TMutation<TResult>::OnCommitted(ECommitResult result)
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
typename TMutation<TResult>::TPtr CreateMutation(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TCallback<TResult()>& mutationAction)
{
    YASSERT(metaStateManager);
    YASSERT(!mutationAction.IsNull());

    TBlob serializedMessage;
    YCHECK(SerializeToProto(&message, &serializedMessage));

    return New< TMutation<TResult> >(
        metaStateManager,
        mutationAction,
        message.GetTypeName(),
        TSharedRef(MoveRV(serializedMessage)));
}

template <class TMessage, class TResult>
typename TMutation<TResult>::TPtr CreateMutation(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TSharedRef& serializedMessage,
    const TCallback<TResult()>& mutationAction)
{
    YASSERT(metaStateManager);

    return New< TMutation<TResult> >(
        metaStateManager,
        mutationAction,
        message.GetTypeName(),
        serializedMessage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
