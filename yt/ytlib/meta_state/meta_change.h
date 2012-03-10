#pragma once

#include "public.h"

#include <ytlib/actions/invoker_util.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
class TMetaChange
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TMetaChange> TPtr;
    typedef IFunc<TResult> TChangeFunc;

    TMetaChange(
        IMetaStateManager* metaStateManager,
        TChangeFunc* func,
        const TSharedRef& changeData);

    typename TFuture<TResult>::TPtr Commit();

    TPtr SetRetriable(TDuration backoffTime);
    TPtr OnSuccess(TIntrusivePtr< IParamAction<TResult> > onSuccess);
    TPtr OnError(TIntrusivePtr<IAction> onError);

private:
    typedef TMetaChange<TResult> TThis;

    IMetaStateManagerPtr MetaStateManager;
    typename TChangeFunc::TPtr Func;
    IAction::TPtr ChangeAction;
    TSharedRef ChangeData;
    bool Started;
    bool Retriable;

    TCancelableContextPtr EpochContext;
    TDuration BackoffTime;
    typename IParamAction<TResult>::TPtr OnSuccess_;
    IAction::TPtr OnError_;
    typename TFuture<TResult>::TPtr AsyncResult;
    TResult Result;

    void DoCommit();
    void ChangeFuncThunk();
    void OnCommitted(ECommitResult result);

};

template <class TTarget, class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    IMetaStateManager* metaStateManager,
    const TMessage& message,
    TResult (TTarget::* func)(const TMessage&),
    TTarget* target);

template <class TMessage, class TResult>
typename TMetaChange<TResult>::TPtr CreateMetaChange(
    IMetaStateManager* metaStateManager,
    const TMessage& message,
    IFunc<TResult>* func);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define META_CHANGE_INL_H_
#include "meta_change-inl.h"
#undef META_CHANGE_INL_H_
