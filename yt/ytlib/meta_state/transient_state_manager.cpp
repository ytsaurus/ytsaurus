#include "stdafx.h"
#include "transient_state_manager.h"

#include <ytlib/actions/action_queue.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TTransientMetaStateManager
    : public IMetaStateManager
{
public:
    TTransientMetaStateManager(IMetaState* metaState)
        : MetaState(metaState)
    {
        StateQueue = New<TActionQueue>("MetaState");
    }

    void Start()
    {
        OnStartLeading_.Fire();
        OnLeaderRecoveryComplete_.Fire();
    }

    void Stop()
    {
        StateQueue->Shutdown();
    }

    EPeerStatus GetControlStatus() const
    {
        return EPeerStatus::Leading;
    }

    EPeerStatus GetStateStatus() const
    {
        return EPeerStatus::Leading;
    }

    bool HasActiveQuorum() const
    {
        return true;
    }

    IInvoker::TPtr GetStateInvoker() const
    {
        return StateQueue->GetInvoker();
    }

    IInvoker::TPtr GetEpochStateInvoker() const
    {
        return StateQueue->GetInvoker();
    }

    TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction = NULL)
    {
        if (!changeAction) {
            MetaState->ApplyChange(changeData);
        } else {
            changeAction->Do();
        }
        return New<TAsyncCommitResult>(ECommitResult::Committed);
    }

    virtual void SetReadOnly(bool readOnly)
    {
        UNUSED(readOnly);
        YUNIMPLEMENTED();
    }

    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
    {
        UNUSED(consumer);
        YUNIMPLEMENTED();
    }

    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStartLeading);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnLeaderRecoveryComplete);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStopLeading);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStartFollowing);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnFollowerRecoveryComplete);
    DEFINE_BYREF_RW_PROPERTY(TSignal, OnStopFollowing);

private:
    TActionQueue::TPtr StateQueue;
    IMetaState::TPtr MetaState;
};

////////////////////////////////////////////////////////////////////////////////

IMetaStateManager::TPtr CreateTransientStateManager(
    IMetaState* metaState)
{
    return New<TTransientMetaStateManager>(metaState);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT


