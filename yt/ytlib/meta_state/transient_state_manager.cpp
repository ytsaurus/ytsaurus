#include "stdafx.h"
#include "transient_state_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TTransientMetaStateManager
    : public IMetaStateManager
{
public:
    TTransientMetaStateManager(
        const IMetaStateManager::TConfig& config,
        IInvoker* controlInvoker,
        IMetaState* metaState)
        : Config(config)
        , Invoker(controlInvoker)
        , MetaState(metaState)
    {  }

    //! Boots up the manager.
    /*!
     * \note Thread affinity: any
     */
    void Start()
    { }

    // TODO: provide stop method

    //! Returns the status as seen in the control thread.
    /*!
     * \note Thread affinity: ControlThread
     */
    EPeerStatus GetControlStatus() const
    {
        return EPeerStatus::Leading;
    }

    //! Returns the status as seen in the state thread.
    /*!
     * \note Thread affinity: StateThread
     */
    EPeerStatus GetStateStatus() const
    {
        return EPeerStatus::Leading;
    }

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvoker::TPtr GetStateInvoker()
    {
        return Invoker;
    }

    //! Returns a cancelable invoker that corresponds to the state thread and is only valid
    //! for the duration of the current epoch.
    /*!
     * \note Thread affinity: StateThread
     */
    IInvoker::TPtr GetEpochStateInvoker()
    {
        return Invoker;
    }

    /*!
     * \note Thread affinity: StateThread
     */
    virtual TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction = NULL)
    {
        if (changeAction == NULL) {
            MetaState->ApplyChange(changeData);
        } else {
            changeAction->Do();
        }
    }

    /*!
     * \note Thread affinity: any
     */
    virtual void SetReadOnly(bool readOnly)
    {
        UNUSED(readOnly);
        YUNIMPLEMENTED();
    }

    //! Returns monitoring info.
    /*!
     * \note Thread affinity: any
     */
    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
    {
        UNUSED(consumer);
        YUNIMPLEMENTED();
    }

    //! Raised within the state thread when the state has started leading
    //! and now enters recovery.
    TSignal& OnStartLeading()
    {
        return Signal;
    }

    //! Raised within the state thread when the leader recovery is complete.
    TSignal& OnLeaderRecoveryComplete()
    {
        return Signal;
    }

    //! Raised within the state thread when the state has stopped leading.
    TSignal& OnStopLeading()
    {
        return Signal;
    }

    //! Raised within the state thread when the state has started following
    //! and now enters recovery.
    TSignal& OnStartFollowing()
    {
        return Signal;
    }

    //! Raised within the state thread when the follower recovery is complete.
    TSignal& OnFollowerRecoveryComplete()
    {
        return Signal;
    }


    //! Raised within the   state thread when the state has started leading.
    TSignal& OnStopFollowing()
    {
        return Signal;
    }

private:
    IInvoker::TPtr Invoker;
    IMetaState::TPtr MetaState;
    IMetaStateManager::TConfig Config;
    TSignal Signal;
};



////////////////////////////////////////////////////////////////////////////////

IMetaStateManager::TPtr CreateTransientStateManager(
    const IMetaStateManager::TConfig& config,
    IInvoker* controlInvoker,
    IMetaState* metaState)
{
    return New<TTransientMetaStateManager>(config, controlInvoker, metaState);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT


