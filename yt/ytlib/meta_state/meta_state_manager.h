#pragma once

#include "common.h"
#include "meta_state.h"

#include <ytlib/actions/signal.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaStateManager
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IMetaStateManager> TPtr;
    typedef TFuture<ECommitResult> TAsyncCommitResult;

    //! Boots up the manager.
    /*!
     * \note Thread affinity: any
     */
    virtual void Start() = 0;

    //! Stops the manager.
    /*!
     * \note Thread affinity: any
     */
    virtual void Stop() = 0;

    //! Returns the status as seen in the control thread.
    /*!
     * \note Thread affinity: ControlThread
     */
    virtual EPeerStatus GetControlStatus() const = 0;

    //! Returns the status as seen in the state thread.
    /*!
     * \note Thread affinity: StateThread
     */
    virtual EPeerStatus GetStateStatus() const = 0;

    //! Returns an invoker used for updating the state.
    /*!
     * \note Thread affinity: any
     */
    virtual IInvoker::TPtr GetStateInvoker() = 0;

    //! Returns a cancelable invoker that corresponds to the state thread and is only valid
    //! for the duration of the current epoch.
    /*!
     * \note Thread affinity: StateThread
     */
    virtual IInvoker::TPtr GetEpochStateInvoker() = 0;

    //! Commits the change.
    /*!
     *  If this is not a leader then #ECommitResult::InvalidStatus is returned.
     *  If this is a leader but without and active quorum, then #ECommitResult::NotCommitted is returned.
     *  If the state is read-only, then #ECommitResult::ReadOnly is returned.
     *  
     *  \param changeData A blob describing the change to be send to followers.
     *  \param changeAction An optional action that is called to perform the changes at the leader,
     *  if NULL then #IMetaState::ApplyChange with #changeData is invoked.
     *
     *  \note Thread affinity: StateThread
     */
    virtual TAsyncCommitResult::TPtr CommitChange(
        const TSharedRef& changeData,
        IAction* changeAction = NULL) = 0;

    //! Toggles read-only mode.
    /*!
     *  \note Thread affinity: any
     */
    virtual void SetReadOnly(bool readOnly) = 0;

    //! Returns monitoring info.
    /*!
     *  \note Thread affinity: any
     */
    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer) = 0;

    //! Raised within the state thread when the state has started leading
    //! and now enters recovery.
    virtual TSignal& OnStartLeading() = 0;
    //! Raised within the state thread when the leader recovery is complete.
    virtual TSignal& OnLeaderRecoveryComplete() = 0;
    //! Raised within the state thread when the state has stopped leading.
    virtual TSignal& OnStopLeading() = 0;

    //! Raised within the state thread when the state has started following
    //! and now enters recovery.
    virtual TSignal& OnStartFollowing() = 0;
    //! Raised within the state thread when the follower recovery is complete.
    virtual TSignal& OnFollowerRecoveryComplete() = 0;
    //! Raised within the   state thread when the state has started leading.
    virtual TSignal& OnStopFollowing() = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
