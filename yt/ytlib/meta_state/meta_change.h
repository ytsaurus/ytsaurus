#pragma once

#include "public.h"

#include <ytlib/actions/cancelable_context.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// This part of the infrastructure is kinda performance-critical so
// we try to pass everything by const&.

template <class TResult>
class TMetaChange
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TMetaChange> TPtr;
    typedef TCallback<TResult()> TChangeFunc;

    TMetaChange(
        const IMetaStateManagerPtr& metaStateManager,
        const TChangeFunc& func,
        const TSharedRef& changeData);

    TFuture<TResult> Commit();

    TPtr SetRetriable(TDuration backoffTime);
    TPtr OnSuccess(const TCallback<void(TResult)>& onSuccess);
    TPtr OnError(const TCallback<void()>& onError);

private:
    typedef TMetaChange<TResult> TThis;

    IMetaStateManagerPtr MetaStateManager;
    TChangeFunc Func;
    TClosure ChangeAction;
    TSharedRef ChangeData;
    bool Started;
    bool Retriable;

    TCancelableContextPtr EpochContext;
    TDuration BackoffTime;
    TCallback<void(TResult)> OnSuccess_;
    TClosure OnError_;
    typename TFuture<TResult>::TPtr AsyncResult;
    TResult Result;

    void DoCommit();
    void ChangeFuncThunk();
    void OnCommitted(ECommitResult result);

};

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    const IMetaStateManagerPtr& metaStateManager,
    const TMessage& message,
    const TCallback<TResult()>& func);

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    const IMetaStateManagerPtr& metaStateManager,
    TRef messageData,
    const TMessage& message,
    const TCallback<TResult()>& func);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define META_CHANGE_INL_H_
#include "meta_change-inl.h"
#undef META_CHANGE_INL_H_
