#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/chunk_tree_statistics.h>

#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/table_server/public.h>
#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/proto/table_ypath.pb.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual const ITabletChunkManagerPtr& GetTabletChunkManager() const = 0;

    virtual void PrepareMount(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze) = 0;
    virtual void PrepareUnmount(
        TTabletOwnerBase* owner,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1) = 0;
    virtual void PrepareRemount(
        TTabletOwnerBase* owner,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1) = 0;
    virtual void PrepareFreeze(
        TTabletOwnerBase* owner,
        int firstTabletIndex,
        int lastTabletIndex) = 0;
    virtual void PrepareUnfreeze(
        TTabletOwnerBase* owner,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1) = 0;
    virtual void PrepareReshard(
        TTabletOwnerBase* owner,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts,
        bool create = false) = 0;

    virtual void CancelTabletTransition(TTablet* tablet) = 0;

    virtual void ValidateMakeTableDynamic(NTableServer::TTableNode* table) = 0;
    virtual void ValidateMakeTableStatic(NTableServer::TTableNode* table) = 0;

    virtual void ValidateCloneTabletOwner(
        TTabletOwnerBase* sourceNode,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) = 0;
    virtual void ValidateSerializeTabletOwner(
        TTabletOwnerBase* sourceNode,
        NCypressServer::ENodeCloneMode mode) = 0;

    virtual void Mount(
        TTabletOwnerBase* table,
        const NYPath::TYPath& path,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze,
        NTransactionClient::TTimestamp mountTimestamp) = 0;
    virtual void Unmount(
        TTabletOwnerBase* table,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1) = 0;
    virtual void Remount(
        TTabletOwnerBase* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1) = 0;
    virtual void Freeze(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex) = 0;
    virtual void Unfreeze(
        TTabletOwnerBase* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1) = 0;
    virtual void Reshard(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts) = 0;

    virtual void SetCustomRuntimeData(
        NTableServer::TTableNode* table,
        NYson::TYsonString data) = 0;

    virtual void CloneTabletOwner(
        TTabletOwnerBase* sourceNode,
        TTabletOwnerBase* clonedNode,
        NCypressServer::ENodeCloneMode mode) = 0;

    virtual void MakeTableDynamic(NTableServer::TTableNode* table, i64 trimmedRowCount = 0) = 0;
    virtual void MakeTableStatic(NTableServer::TTableNode* table) = 0;

    virtual void AlterTableReplica(
        TTableReplica* replica,
        std::optional<bool> enabled,
        std::optional<ETableReplicaMode> mode,
        std::optional<NTransactionClient::EAtomicity> atomicity,
        std::optional<bool> preserveTimestamps,
        std::optional<bool> enableReplicatedTableTracker) = 0;

    virtual void LockDynamicTable(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction,
        NTransactionClient::TTimestamp timestamp) = 0;
    virtual void CheckDynamicTableLock(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction,
        NTableClient::NProto::TRspCheckDynamicTableLock* response) = 0;

    virtual std::vector<TTabletActionId> SyncBalanceCells(
        TTabletCellBundle* bundle,
        const std::optional<std::vector<NTableServer::TTableNode*>>& tables,
        bool keepActions) = 0;
    virtual std::vector<TTabletActionId> SyncBalanceTablets(
        NTableServer::TTableNode* table,
        bool keepActions) = 0;

    virtual void MergeTable(
        NTableServer::TTableNode* originatingNode,
        NTableServer::TTableNode* branchedNode) = 0;

    virtual void OnNodeStorageParametersUpdated(NChunkServer::TChunkOwnerBase* node) = 0;

    virtual TTabletCellBundle* FindTabletCellBundle(TTabletCellBundleId id) = 0;
    virtual TTabletCellBundle* GetTabletCellBundleOrThrow(TTabletCellBundleId id, bool activeLifeStageOnly) = 0;
    virtual TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const std::string& name, bool activeLifeStageOnly) = 0;
    virtual TTabletCellBundle* GetDefaultTabletCellBundle() = 0;
    virtual void SetTabletCellBundle(TTabletOwnerBase* table, TTabletCellBundle* cellBundle) = 0;

    virtual TTabletCell* GetTabletCellOrThrow(TTabletCellId id) = 0;
    virtual void ZombifyTabletCell(TTabletCell* cell) = 0;

    virtual void DestroyTablet(TTabletBase* tablet) = 0;

    virtual void DestroyTabletOwner(TTabletOwnerBase* table) = 0;

    virtual NNodeTrackerServer::TNode* FindTabletLeaderNode(const TTabletBase* tablet) const = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Tablet, TTabletBase);
    virtual TTabletBase* GetTabletOrThrow(TTabletId id) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

    virtual void RecomputeTabletCellStatistics(NCellServer::TCellBase* cellBase) = 0;

    virtual void OnHunkJournalChunkSealed(NChunkServer::TChunk* chunk) = 0;

    virtual void AttachDynamicStoreToTablet(TTablet* tablet, NChunkServer::TDynamicStore* dynamicStore) = 0;

    virtual void FireUponTableReplicaUpdate(TTableReplica* replica) = 0;

    DECLARE_INTERFACE_SIGNAL_WITH_ACCESSOR(void(TReplicatedTableData), ReplicatedTableCreated);
    DECLARE_INTERFACE_SIGNAL_WITH_ACCESSOR(void(NTableClient::TTableId), ReplicatedTableDestroyed);

    DECLARE_INTERFACE_SIGNAL(void(TReplicaData), ReplicaCreated);
    DECLARE_INTERFACE_SIGNAL(void(NTabletClient::TTableReplicaId), ReplicaDestroyed);

private:
    template <class TImpl>
    friend class NTableServer::TTableNodeTypeHandlerBase;
    friend class TTabletTypeHandler;
    friend class TTabletCellTypeHandler;
    friend class TTabletCellBundleTypeHandler;
    friend class TTableReplicaTypeHandler;
    friend class TTabletActionTypeHandler;
    friend class TTabletBalancer;

    virtual TTableReplica* CreateTableReplica(
        NTableServer::TReplicatedTableNode* table,
        const std::string& clusterName,
        const NYPath::TYPath& replicaPath,
        ETableReplicaMode mode,
        bool preserveTimestamps,
        NTransactionClient::EAtomicity atomicity,
        bool enabled,
        NTransactionClient::TTimestamp startReplicationTimestamp,
        const std::optional<std::vector<i64>>& startReplicationRowIndexes,
        bool enableReplicatedTableTracker) = 0;
    virtual void ZombifyTableReplica(TTableReplica* replica) = 0;

    virtual TTabletAction* CreateTabletAction(
        NObjectClient::TObjectId hintId,
        ETabletActionKind kind,
        const std::vector<TTabletBaseRawPtr>& tablets,
        const std::vector<TTabletCellRawPtr>& cells,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::optional<int>& tabletCount,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime,
        std::optional<TDuration> expirationTimeout) = 0;
    virtual void ZombifyTabletAction(TTabletAction* action) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletManager)

////////////////////////////////////////////////////////////////////////////////

ITabletManagerPtr CreateTabletManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
