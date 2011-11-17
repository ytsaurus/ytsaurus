#pragma once

#include "common.h"
#include "meta_state.h"

#include "../rpc/server.h"
#include "../actions/signal.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaStateManager> TPtr;
    typedef TMetaStateManagerConfig TConfig;

    typedef TFuture<ECommitResult> TAsyncCommitResult;

    TMetaStateManager(
        const TConfig& config,
        IInvoker* controlInvoker,
        IMetaState* metaState,
        NRpc::IServer* server);

    ~TMetaStateManager();

    //! Boots up the manager.
    /*!
     * \note Thread affinity: any
     */
    void Start();

    // TODO: provide stop method

    //! Returns the status as seen in the control thread.
    /*!
     * \note Thread affinity: ControlThread
     */
    EPeerStatus GetControlStatus() const;

    //! Returns the status as seen in the state thread.
    /*!
     * \note Thread affinity: StateThread
     */
    EPeerStatus GetStateStatus() const;

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvoker::TPtr GetStateInvoker();

    //! Returns a cancelable invoker that corresponds to the state thread and is only valid
    //! for the duration of the current epoch.
    /*!
     * \note Thread affinity: StateThread
     */
    IInvoker::TPtr GetEpochStateInvoker();

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    IInvoker::TPtr GetSnapshotInvoker();

    /*!
     * \note Thread affinity: StateThread
     */
    TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction = NULL);

    /*!
     * \note Thread affinity: any
     */
    void SetReadOnly(bool readOnly);

    //! Returns monitoring info.
    /*!
     * \note Thread affinity: any
     */
    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    //! Raised within the state thread when the state has started leading
    //! and now enters recovery.
    TSignal& OnStartLeading();
    //! Raised within the state thread when the leader recovery is complete.
    TSignal& OnLeaderRecoveryComplete();
    //! Raised within the state thread when the state has stopped leading.
    TSignal& OnStopLeading();

    //! Raised within the state thread when the state has started following
    //! and now enters recovery.
    TSignal& OnStartFollowing();
    //! Raised within the state thread when the follower recovery is complete.
    TSignal& OnFollowerRecoveryComplete();
    //! Raised within the   state thread when the state has started leading.
    TSignal& OnStopFollowing();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
