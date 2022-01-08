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
    TString Name;
    //! Additional attributes for the lock transaction.
    NYTree::IAttributeDictionaryPtr TransactionAttributes = nullptr;
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
    virtual void Stop() = 0;

    //! Immediately stops leading.
    virtual void StopLeading() = 0;

    //! Returns id of a prerequisite transaction for current epoch.
    //! If instance is not leader now, returns |NullTransactionId|.
    virtual NTransactionClient::TTransactionId GetPrerequistiveTransactionId() const = 0;

    virtual bool IsLeader() const = 0;

    //! Fired when instance became leader.
    DECLARE_INTERFACE_SIGNAL(void(), LeadingStarted);
    //! Fired when instance leading ends.
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
