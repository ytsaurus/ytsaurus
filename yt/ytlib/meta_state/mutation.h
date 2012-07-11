#pragma once

#include "public.h"

#include <ytlib/actions/cancelable_context.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// This part of the infrastructure is kinda performance-critical so
// we try to pass everything by const&.

template <class TResult>
class TMutation
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TMutation> TPtr;
    typedef TCallback<TResult()> TMutationAction;

    TMutation(
        const IMetaStateManagerPtr& metaStateManager,
        const TMutationAction& mutationAction,
        const Stroka& mutationType,
        const TSharedRef& mutationData);

    TFuture<TResult> Commit();
    TFuture<TResult> PostCommit();

    TPtr SetRetriable(TDuration backoffTime);
    TPtr OnSuccess(const TCallback<void(TResult)>& onSuccess);
    TPtr OnError(const TCallback<void()>& onError);

private:
    typedef TMutation<TResult> TThis;

    IMetaStateManagerPtr MetaStateManager;
    TMutationAction MutationAction;
    Stroka MutationType;
    TSharedRef MutationData;
    bool Started;
    bool Retriable;

    TCancelableContextPtr EpochContext;
    TDuration BackoffTime;
    TCallback<void(TResult)> OnSuccess_;
    TClosure OnError_;
    TPromise<TResult> Promise;
    TResult Result;

    void DoCommit();
    void MutationActionThunk();
    void OnCommitted(ECommitResult result);

};

template <class TMessage, class TResult>
typename TMutation<TResult>::TPtr CreateMutation(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TCallback<TResult()>& mutationAction);

template <class TMessage, class TResult>
typename TMutation<TResult>::TPtr CreateMutation(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TSharedRef& serializedMessage,
    const TCallback<TResult()>& mutationAction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define MUTATION_INL_H_
#include "mutation-inl.h"
#undef MUTATION_INL_H_
