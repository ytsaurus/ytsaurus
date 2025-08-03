#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_action.h>
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
        TTypeErasedTransactionActionDescriptor descriptor) = 0;
    template <class TProto, class TState = void>
    void RegisterTransactionActionHandlers(
        TTypedTransactionActionDescriptor<TProto, TState> descriptor);

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

#define TRANSACTION_MANAGER_INL_H_
#include "transaction_manager-inl.h"
#undef TRANSACTION_MANAGER_INL_H_
