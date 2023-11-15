#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IChaosManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    using TCtxGenerateReplicationCardIdPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqGenerateReplicationCardId,
        NChaosClient::NProto::TRspGenerateReplicationCardId
    >>;
    using TCtxCreateReplicationCardPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqCreateReplicationCard,
        NChaosClient::NProto::TRspCreateReplicationCard
    >>;
    using TCtxRemoveReplicationCardPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqRemoveReplicationCard,
        NChaosClient::NProto::TRspRemoveReplicationCard
    >>;
    using TCtxCreateTableReplicaPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqCreateTableReplica,
        NChaosClient::NProto::TRspCreateTableReplica
    >>;
    using TCtxRemoveTableReplicaPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqRemoveTableReplica,
        NChaosClient::NProto::TRspRemoveTableReplica
    >>;
    using TCtxAlterTableReplicaPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqAlterTableReplica,
        NChaosClient::NProto::TRspAlterTableReplica
    >>;
    using TCtxUpdateTableReplicaProgressPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqUpdateTableReplicaProgress,
        NChaosClient::NProto::TRspUpdateTableReplicaProgress
    >>;
    using TCtxAlterReplicationCardPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqAlterReplicationCard,
        NChaosClient::NProto::TRspAlterReplicationCard
    >>;
    using TCtxMigrateReplicationCardsPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqMigrateReplicationCards,
        NChaosClient::NProto::TRspMigrateReplicationCards
    >>;
    using TCtxResumeChaosCellPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqResumeChaosCell,
        NChaosClient::NProto::TRspResumeChaosCell
    >>;
    using TCtxCreateReplicationCardCollocationPtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqCreateReplicationCardCollocation,
        NChaosClient::NProto::TRspCreateReplicationCardCollocation
    >>;

    virtual void GenerateReplicationCardId(const TCtxGenerateReplicationCardIdPtr& context) = 0;
    virtual void CreateReplicationCard(const TCtxCreateReplicationCardPtr& context) = 0;
    virtual void RemoveReplicationCard(const TCtxRemoveReplicationCardPtr& context) = 0;
    virtual void CreateTableReplica(const TCtxCreateTableReplicaPtr& context) = 0;
    virtual void RemoveTableReplica(const TCtxRemoveTableReplicaPtr& context) = 0;
    virtual void AlterTableReplica(const TCtxAlterTableReplicaPtr& context) = 0;
    virtual void UpdateTableReplicaProgress(const TCtxUpdateTableReplicaProgressPtr& context) = 0;
    virtual void AlterReplicationCard(const TCtxAlterReplicationCardPtr& context) = 0;
    virtual void MigrateReplicationCards(const TCtxMigrateReplicationCardsPtr& context) = 0;
    virtual void ResumeChaosCell(const TCtxResumeChaosCellPtr& context) = 0;
    virtual TFuture<void> ExecuteAlterTableReplica(const NChaosClient::NProto::TReqAlterTableReplica& request) = 0;
    virtual void CreateReplicationCardCollocation(const TCtxCreateReplicationCardCollocationPtr& context) = 0;

    virtual const std::vector<NObjectClient::TCellId>& CoordinatorCellIds() = 0;
    virtual bool IsCoordinatorSuspended(NObjectClient::TCellId coordinatorCellId) = 0;

    DECLARE_INTERFACE_SIGNAL(void(NTabletServer::TReplicatedTableData), ReplicatedTableCreated);
    DECLARE_INTERFACE_SIGNAL(void(NTableClient::TTableId), ReplicatedTableDestroyed);
    DECLARE_INTERFACE_SIGNAL(void(NTabletServer::TReplicaData), ReplicaCreated);
    DECLARE_INTERFACE_SIGNAL(void(NTabletClient::TTableReplicaId), ReplicaDestroyed);
    DECLARE_INTERFACE_SIGNAL(void(NTabletServer::TTableCollocationData), ReplicationCollocationCreated);
    DECLARE_INTERFACE_SIGNAL(void(NTableClient::TTableCollocationId), ReplicationCollocationDestroyed);

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ReplicationCard, TReplicationCard);
    virtual TReplicationCard* GetReplicationCardOrThrow(TReplicationCardId replicationCardId, bool allowMigrated=false) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ReplicationCardCollocation, TReplicationCardCollocation);
};

DEFINE_REFCOUNTED_TYPE(IChaosManager)

IChaosManagerPtr CreateChaosManager(
    TChaosManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
