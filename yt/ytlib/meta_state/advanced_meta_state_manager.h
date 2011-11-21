#pragma once

#include "meta_state_manager.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TAdvancedMetaStateManager
    : public IMetaStateManager
{
public:
    typedef TIntrusivePtr<TAdvancedMetaStateManager> TPtr;
    
    TAdvancedMetaStateManager(
        const TConfig& config,
        IInvoker* controlInvoker,
        IMetaState* metaState,
        NRpc::IServer* server);

    //! Boots up the manager.
    /*!
     * \note Thread affinity: any
     */
    virtual void Start();

    // TODO: provide stop method

    //! Returns the status as seen in the control thread.
    /*!
     * \note Thread affinity: ControlThread
     */
    virtual EPeerStatus GetControlStatus() const;

    //! Returns the status as seen in the state thread.
    /*!
     * \note Thread affinity: StateThread
     */
    virtual EPeerStatus GetStateStatus() const;

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    virtual IInvoker::TPtr GetStateInvoker();

    //! Returns a cancelable invoker that corresponds to the state thread and is only valid
    //! for the duration of the current epoch.
    /*!
     * \note Thread affinity: StateThread
     */
    virtual IInvoker::TPtr GetEpochStateInvoker();

    /*!
     * \note Thread affinity: StateThread
     */
    virtual TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction = NULL);

    /*!
     * \note Thread affinity: any
     */
    virtual void SetReadOnly(bool readOnly);

    //! Returns monitoring info.
    /*!
     * \note Thread affinity: any
     */
    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    //! Raised within the state thread when the state has started leading
    //! and now enters recovery.
    virtual TSignal& OnStartLeading();
    //! Raised within the state thread when the leader recovery is complete.
    virtual TSignal& OnLeaderRecoveryComplete();
    //! Raised within the state thread when the state has stopped leading.
    virtual TSignal& OnStopLeading();

    //! Raised within the state thread when the state has started following
    //! and now enters recovery.
    virtual TSignal& OnStartFollowing();
    //! Raised within the state thread when the follower recovery is complete.
    virtual TSignal& OnFollowerRecoveryComplete();
    //! Raised within the   state thread when the state has started leading.
    virtual TSignal& OnStopFollowing();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
