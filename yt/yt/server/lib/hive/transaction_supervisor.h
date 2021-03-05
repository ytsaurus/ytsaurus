#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/security_server/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NHiveServer {

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

    virtual void Decommission() = 0;
    virtual bool IsDecommissioned() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionSupervisor)

ITransactionSupervisorPtr CreateTransactionSupervisor(
    TTransactionSupervisorConfigPtr config,
    IInvokerPtr automatonInvoker,
    IInvokerPtr trackerInvoker,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NRpc::TResponseKeeperPtr responseKeeper,
    ITransactionManagerPtr transactionManager,
    TCellId selfCellId,
    NTransactionClient::ITimestampProviderPtr timestampProvider,
    std::vector<ITransactionParticipantProviderPtr> participantProviders);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
