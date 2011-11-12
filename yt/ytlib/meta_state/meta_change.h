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
        TMetaStateManager* metaStateManager,
        TChangeFunc* func,
        const TSharedRef& changeData,
        ECommitMode mode);

    typename TFuture<TResult>::TPtr Commit();
    
    TPtr OnSuccess(typename IParamAction<TResult>::TPtr onSuccess);
    TPtr OnError(IAction::TPtr onError);

private:
    typedef TMetaChange<TResult> TThis;

    TMetaStateManager::TPtr MetaStateManager;
    typename TChangeFunc::TPtr ChangeFunc;
    TSharedRef ChangeData;
    ECommitMode CommitMode;

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
    TMetaStateManager* metaStateManager,
    const TMessage& message,
    TResult (TTarget::* func)(const TMessage&),
    TTarget* target,
    ECommitMode mode = ECommitMode::NeverFails);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define META_CHANGE_INL_H_
#include "meta_change-inl.h"
#undef META_CHANGE_INL_H_
