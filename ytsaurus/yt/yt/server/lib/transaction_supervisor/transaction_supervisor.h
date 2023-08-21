#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/server/lib/security_server/public.h>

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
};

DEFINE_REFCOUNTED_TYPE(ITransactionSupervisor)

ITransactionSupervisorPtr CreateTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    IInvokerPtr trackerInvoker,
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
