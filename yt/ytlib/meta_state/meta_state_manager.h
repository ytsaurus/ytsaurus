#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>
#include <ytlib/actions/cancelable_context.h>
#include <ytlib/misc/ref.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaStateManager
    : public virtual TRefCounted
{
    //! Boots up the manager.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Start() = 0;

    //! Stops the manager.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Stop() = 0;

    //! Returns the status as seen in the control thread.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual EPeerStatus GetControlStatus() const = 0;

    //! Returns the status as seen in the state thread.
    /*!
     *  \note Thread affinity: StateThread
     */
    virtual EPeerStatus GetStateStatus() const = 0;

    //! Similar to #GetStateStatus but can be called from any thread.
    /*!
     *  \note Thread affinity: any
     */
    virtual EPeerStatus GetStateStatusAsync() const = 0;

    //! Returns an invoker used for updating the state.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr GetStateInvoker() const = 0;

    //! Returns True is the peer has a active quorum.
    /*!
     *  \note Thread affinity: any
     */
    virtual bool HasActiveQuorum() const = 0;

    //! Returns a cancelable context representing the current epoch.
    /*!
     *  \note Thread affinity: any
     */
    virtual TCancelableContextPtr GetEpochContext() const = 0;

    //! Commits the change.
    /*!
     *  If the peer is not the leader then #ECommitResult::InvalidStatus is returned.
     *  If the peer is the leader but has no active quorum, then #ECommitResult::NotCommitted is returned.
     *  If the state is read-only, then #ECommitResult::ReadOnly is returned.
     *  
     *  \param changeData A blob describing the change to be send to followers.
     *  \param changeAction An optional action that is called to perform the changes at the leader,
     *  if NULL then #IMetaState::ApplyChange is invoked with #changeData.
     *
     *  \note Thread affinity: StateThread
     */
    virtual TAsyncCommitResult CommitChange(
        const TSharedRef& changeData,
        TClosure changeAction) = 0;

    //! Returns True if #CommitChange is currently in progress.
    /*!
     *  This is typically used to prevent recursive commits and only log "top-level"
     *  changes that trigger the whole transformation chain.
     *  
     *  \note Thread affinity: StateThread
     */
    virtual bool IsInCommit() const = 0;

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
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartLeading);
    //! Raised within the state thread when the leader recovery is complete.
    DECLARE_INTERFACE_SIGNAL(void(), LeaderRecoveryComplete);
    //! Raised within the state thread when the state has stopped leading.
    DECLARE_INTERFACE_SIGNAL(void(), StopLeading);

    //! Raised within the state thread when the state has started following
    //! and enters recovery.
    DECLARE_INTERFACE_SIGNAL(void(), StartFollowing);
    //! Raised within the state thread when the follower recovery is complete.
    DECLARE_INTERFACE_SIGNAL(void(), FollowerRecoveryComplete);
    //! Raised within the   state thread when the state has started leading.
    DECLARE_INTERFACE_SIGNAL(void(), StopFollowing);

    // Extension methods.
    bool IsLeader() const;
    bool IsFolllower() const;
    bool IsRecovery() const;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
