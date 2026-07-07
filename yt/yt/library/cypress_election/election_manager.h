#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/library/lock_election/election_manager.h>

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

struct TCypressElectionManagerOptions
    : public TRefCounted
{
    std::string GroupName;
    std::string MemberName;
    //! Additional attributes for the lock transaction.
    NYTree::IAttributeDictionaryPtr TransactionAttributes;
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionManagerOptions)

////////////////////////////////////////////////////////////////////////////////

struct ICypressElectionManager
    : public NLockElection::ILockElectionManager
{
    //! Returns id of a prerequisite transaction for current epoch.
    //! If instance is not leader now, returns |NullTransactionId|.
    virtual NTransactionClient::TTransactionId GetPrerequisiteTransactionId() const = 0;

    //! Return the attributes of the leader's transaction.
    //! NB: Might be stale.
    //! NB: Can return null when the instance is just starting up and has no information yet.
    virtual NYTree::IAttributeDictionaryPtr GetCachedLeaderTransactionAttributes() const = 0;
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
