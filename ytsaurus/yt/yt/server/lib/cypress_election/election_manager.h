#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

struct TCypressElectionManagerOptions
    : public TRefCounted
{
    TString GroupName;
    TString MemberName;
    //! Additional attributes for the lock transaction.
    NYTree::IAttributeDictionaryPtr TransactionAttributes;
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionManagerOptions)

////////////////////////////////////////////////////////////////////////////////

struct ICypressElectionManager
    : public TRefCounted
{
    //! Start participating in leader election.
    virtual void Start() = 0;
    //! Stops participating in leader election.
    //! If instance is a leader prior to this call
    //! leading is stopped.
    virtual TFuture<void> Stop() = 0;
    //! Return whether this instance is participating in leader election.
    virtual bool IsActive() const = 0;

    //! Immediately stops leading.
    virtual TFuture<void> StopLeading() = 0;

    //! Returns id of a prerequisite transaction for current epoch.
    //! If instance is not leader now, returns |NullTransactionId|.
    virtual NTransactionClient::TTransactionId GetPrerequisiteTransactionId() const = 0;

    //! Return whether this instance is currently leading.
    //! NB: Might be stale.
    virtual bool IsLeader() const = 0;
    //! Return the attributes of the leader's transaction.
    //! NB: Might be stale.
    //! NB: Can return null when the instance is just starting up and has no information yet.
    virtual NYTree::IAttributeDictionaryPtr GetCachedLeaderTransactionAttributes() const = 0;

    //! Fired when instance became leader.
    //! NB: Signal handler should not throw or make context switches.
    DECLARE_INTERFACE_SIGNAL(void(), LeadingStarted);
    //! Fired when instance leading ends.
    //! NB: Signal handler should not throw or make context switches.
    DECLARE_INTERFACE_SIGNAL(void(), LeadingEnded);
};

DEFINE_REFCOUNTED_TYPE(ICypressElectionManager)

////////////////////////////////////////////////////////////////////////////////

ICypressElectionManagerPtr CreateCypressElectionManager(
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    TCypressElectionManagerConfigPtr config,
    TCypressElectionManagerOptionsPtr options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
