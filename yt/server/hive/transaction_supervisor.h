#pragma once

#include "public.h"

#include <yt/server/hydra/public.h>

#include <yt/server/security_server/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisor
    : public TRefCounted
{
public:
    TTransactionSupervisor(
        TTransactionSupervisorConfigPtr config,
        IInvokerPtr automatonInvoker,
        IInvokerPtr trackerInvoker,
        NHydra::IHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        NRpc::TResponseKeeperPtr responseKeeper,
        ITransactionManagerPtr transactionManager,
        NSecurityServer::ISecurityManagerPtr securityManager,
        const TCellId& selfCellId,
        NTransactionClient::ITimestampProviderPtr timestampProvider,
        const std::vector<ITransactionParticipantProviderPtr>& participantProviders);

    ~TTransactionSupervisor();

    std::vector<NRpc::IServicePtr> GetRpcServices();

    TFuture<void> CommitTransaction(
        const TTransactionId& transactionId,
        const TString& user,
        const std::vector<NHydra::TCellId>& participantCellIds = std::vector<NHydra::TCellId>());

    TFuture<void> AbortTransaction(
        const TTransactionId& transactionId,
        bool force = false);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
