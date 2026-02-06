#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/security_server/public.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionSupervisor
    : public virtual TRefCounted
{
    virtual std::vector<NRpc::IServicePtr> GetRpcServices() = 0;

    virtual TFuture<void> CommitTransaction(
        TTransactionId transactionId) = 0;

    virtual TFuture<void> AbortTransaction(
        TTransactionId transactionId,
        bool force = false) = 0;

    virtual void SetDecommission(bool decommission) = 0;

    //! Returns true if transaction supervisor is decommissioned
    //! and there are no more alive transactions in it, so it can
    //! be safely removed.
    virtual bool IsDecommissioned() const = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    //! Returns future which is set when all currently prepared transactions are
    //! finished.
    /*!
     *  The main usage of this function is to ensure that effects of committed
     *  (from client's point of view) Sequoia transaction will be observed:
     *  coordinator replies to commit request after transaction is prepared on
     *  every participant but not necessary committed.
     *
     *  Note that this method isn't needed to observe effects of transactions
     *  coordinated by current cell due to late prepare: cordinator prepare
     *  happens in the same mutation as commit.
     */
    virtual TFuture<void> WaitUntilPreparedTransactionsFinished() = 0;

    virtual TTimestamp GetLastCoordinatorCommitTimestamp() = 0;

    //! COMPAT(aleksandra-zh).
    virtual void RecomputeStronglyOrderedTransactionRefsOnCoordinator() = 0;

    virtual void OnProfiling(NProfiling::TSensorBuffer* buffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionSupervisor)

ITransactionSupervisorPtr CreateTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NRpc::IResponseKeeperPtr responseKeeper,
    ITransactionManagerPtr transactionManager,
    TCellId selfCellId,
    NApi::TClusterTag selfClockClusterTag,
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    std::vector<ITransactionParticipantProviderPtr> participantProviders,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
