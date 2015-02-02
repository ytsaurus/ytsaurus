#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/actions/public.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor
    : public TRefCounted
{
public:
    TTransactionSupervisor(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        NRpc::IResponseKeeperPtr responseKeeper,
        THiveManagerPtr hiveManager,
        ITransactionManagerPtr transactionManager,
        NTransactionClient::ITimestampProviderPtr timestampProvider);

    ~TTransactionSupervisor();

    NRpc::IServicePtr GetRpcService();

    TFuture<void> CommitTransaction(
        const TTransactionId& transactionId,
        const std::vector<NHydra::TCellId>& participantCellIds = std::vector<NHydra::TCellId>());

    TFuture<void> AbortTransaction(
        const TTransactionId& transactionId,
        bool force = false);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
