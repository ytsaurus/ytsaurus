#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

using TCreateReplicationCardContextPtr = TIntrusivePtr<::NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqCreateReplicationCard,
    NYT::NChaosClient::NProto::TRspCreateReplicationCard>>;
using TCreateTableReplicaContextPtr = TIntrusivePtr<::NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqCreateTableReplica,
    NYT::NChaosClient::NProto::TRspCreateTableReplica>>;
using TRemoveTableReplicaContextPtr = TIntrusivePtr<::NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqRemoveTableReplica,
    NYT::NChaosClient::NProto::TRspRemoveTableReplica>>;
using TAlterTableReplicaContextPtr = TIntrusivePtr<::NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqAlterTableReplica,
    NYT::NChaosClient::NProto::TRspAlterTableReplica>>;
using TUpdateReplicationProgressContextPtr = TIntrusivePtr<::NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqUpdateReplicationProgress,
    NYT::NChaosClient::NProto::TRspUpdateReplicationProgress>>;

////////////////////////////////////////////////////////////////////////////////

struct IChaosManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void CreateReplicationCard(const TCreateReplicationCardContextPtr& context) = 0;
    virtual void CreateTableReplica(const TCreateTableReplicaContextPtr& context) = 0;
    virtual void RemoveTableReplica(const TRemoveTableReplicaContextPtr& context) = 0;
    virtual void AlterTableReplica(const TAlterTableReplicaContextPtr& context) = 0;
    virtual void UpdateReplicationProgress(const TUpdateReplicationProgressContextPtr& context) = 0;

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
