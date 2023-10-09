#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>

#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionManager
    : public NTransactionSupervisor::ITransactionManager
{
    using TCtxRegisterTransactionActions = NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqRegisterTransactionActions,
        NChaosClient::NProto::TRspRegisterTransactionActions>;
    using TCtxRegisterTransactionActionsPtr = TIntrusivePtr<TCtxRegisterTransactionActions>;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual void RegisterTransactionActionHandlers(
        const NTransactionSupervisor::TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const NTransactionSupervisor::TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const NTransactionSupervisor::TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor) = 0;

    virtual std::unique_ptr<NHydra::TMutation> CreateRegisterTransactionActionsMutation(
        TCtxRegisterTransactionActionsPtr context) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Transaction, TTransaction);
};

DEFINE_REFCOUNTED_TYPE(ITransactionManager)

////////////////////////////////////////////////////////////////////////////////

ITransactionManagerPtr CreateTransactionManager(
    TTransactionManagerConfigPtr config,
    IChaosSlotPtr slot,
    NApi::TClusterTag clockClusterTag,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
