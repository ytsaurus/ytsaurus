#pragma once

#include "meta_state_manager.h"

#include "../actions/action.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
class TMetaChange
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaChange> TPtr;
    typedef IFunc<TResult> TChangeFunc;

    TMetaChange(
        IMetaStateManager* metaStateManager,
        TChangeFunc* func,
        const TSharedRef& changeData);

    typename TFuture<TResult>::TPtr Commit();
    
    TPtr OnSuccess(IParamAction<TResult>* onSuccess);
    TPtr OnError(IAction* onError);

private:
    typedef TMetaChange<TResult> TThis;

    IMetaStateManager::TPtr MetaStateManager;
    typename TChangeFunc::TPtr Func;
    IAction::TPtr ChangeAction;
    TSharedRef ChangeData;

    typename IParamAction<TResult>::TPtr OnSuccess_;
    IAction::TPtr OnError_;

    bool Started;
    typename TFuture<TResult>::TPtr AsyncResult;
    TResult Result;

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
