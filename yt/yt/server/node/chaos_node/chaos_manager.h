#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IChaosManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    using TCreateReplicationCardContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqCreateReplicationCard,
        NChaosClient::NProto::TRspCreateReplicationCard
    >>;
    using TRemoveReplicationCardContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqRemoveReplicationCard,
        NChaosClient::NProto::TRspRemoveReplicationCard
    >>;
    using TCreateTableReplicaContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqCreateTableReplica,
        NChaosClient::NProto::TRspCreateTableReplica
    >>;
    using TRemoveTableReplicaContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqRemoveTableReplica,
        NChaosClient::NProto::TRspRemoveTableReplica
    >>;
    using TAlterTableReplicaContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqAlterTableReplica,
        NChaosClient::NProto::TRspAlterTableReplica
    >>;
    using TUpdateTableReplicaProgressContextPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqUpdateTableReplicaProgress,
        NChaosClient::NProto::TRspUpdateTableReplicaProgress
    >>;

    virtual void CreateReplicationCard(const TCreateReplicationCardContextPtr& context) = 0;
    virtual void RemoveReplicationCard(const TRemoveReplicationCardContextPtr& context) = 0;
    virtual void CreateTableReplica(const TCreateTableReplicaContextPtr& context) = 0;
    virtual void RemoveTableReplica(const TRemoveTableReplicaContextPtr& context) = 0;
    virtual void AlterTableReplica(const TAlterTableReplicaContextPtr& context) = 0;
    virtual void UpdateTableReplicaProgress(const TUpdateTableReplicaProgressContextPtr& context) = 0;

    virtual const std::vector<NObjectClient::TCellId>& CoordinatorCellIds() = 0;
    virtual bool IsCoordinatorSuspended(NObjectClient::TCellId coordinatorCellId) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ReplicationCard, TReplicationCard);
    virtual TReplicationCard* GetReplicationCardOrThrow(TReplicationCardId replicationCardId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosManager)

IChaosManagerPtr CreateChaosManager(
    TChaosManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
