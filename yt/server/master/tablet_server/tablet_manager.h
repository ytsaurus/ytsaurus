#pragma once

#include "public.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "table_replica.h"
#include "tablet_action.h"
#include "tablet_action_type_handler.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/chunk_server/chunk_tree_statistics.h>

#include <yt/server/lib/hydra/entity_map.h>
#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/table_server/public.h>
#include <yt/server/master/table_server/table_node.h>

#include <yt/server/master/node_tracker_server/public.h>

#include <yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/proto/table_ypath.pb.h>

#include <yt/core/misc/small_vector.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletManager
    : public TRefCounted
{
public:
    explicit TTabletManager(
        TTabletManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();

    TTabletStatistics GetTabletStatistics(const TTablet* tablet);

    void PrepareMountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze,
        NTransactionClient::TTimestamp mountTimestamp);
    void PrepareUnmountTable(
        NTableServer::TTableNode* table,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void PrepareRemountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void PrepareFreezeTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex);
    void PrepareUnfreezeTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void PrepareReshardTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        bool create = false);

    void ValidateMakeTableDynamic(NTableServer::TTableNode* table);
    void ValidateMakeTableStatic(NTableServer::TTableNode* table);

    void ValidateCloneTable(
        NTableServer::TTableNode* sourceTable,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account);
    void ValidateBeginCopyTable(
        NTableServer::TTableNode* sourceTable,
        NCypressServer::ENodeCloneMode mode);

    void MountTable(
        NTableServer::TTableNode* table,
        const TString& path,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze,
        NTransactionClient::TTimestamp mountTimestamp);
    void UnmountTable(
        NTableServer::TTableNode* table,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void RemountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void FreezeTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex);
    void UnfreezeTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void ReshardTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TOwningKey>& pivotKeys);
    void CloneTable(
        NTableServer::TTableNode* sourceTable,
        NTableServer::TTableNode* clonedTable,
        NCypressServer::ENodeCloneMode mode);

    void MakeTableDynamic(NTableServer::TTableNode* table);
    void MakeTableStatic(NTableServer::TTableNode* table);

    void AlterTableReplica(
        TTableReplica* replica,
        std::optional<bool> enabled,
        std::optional<ETableReplicaMode> mode,
        std::optional<NTransactionClient::EAtomicity> atomicity,
        std::optional<bool> preserveTimestamps);

    void LockDynamicTable(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction,
        NTransactionClient::TTimestamp timestamp);
    void CheckDynamicTableLock(
        NTableServer::TTableNode* table,
        NTransactionServer::TTransaction* transaction,
        NTableClient::NProto::TRspCheckDynamicTableLock* response);

    std::vector<TTabletActionId> SyncBalanceCells(
        TTabletCellBundle* bundle,
        const std::optional<std::vector<NTableServer::TTableNode*>>& tables,
        bool keepActions);
    std::vector<TTabletActionId> SyncBalanceTablets(
        NTableServer::TTableNode* table,
        bool keepActions);

    const TBundleNodeTrackerPtr& GetBundleNodeTracker();

    void MergeTable(
        NTableServer::TTableNode* originatingNode,
        NTableServer::TTableNode* branchedNode);

    void SendTableStatisticsUpdates(NChunkServer::TChunkOwnerBase* chunkOwniner);

    TTabletCellBundle* FindTabletCellBundle(TTabletCellBundleId id);
    TTabletCellBundle* GetTabletCellBundleOrThrow(TTabletCellBundleId id);
    TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const TString& name);
    TTabletCellBundle* GetDefaultTabletCellBundle();
    void SetTabletCellBundle(NTableServer::TTableNode* table, TTabletCellBundle* cellBundle);
    void SetTabletCellBundle(NCypressServer::TCompositeNodeBase* node, TTabletCellBundle* cellBundle);
    void SetTabletCellBundleOptions(TTabletCellBundle* cellBundle, TTabletCellOptionsPtr options);

    TTabletCell* GetTabletCellOrThrow(TTabletCellId id);
    void ZombifyTabletCell(TTabletCell* cell);

    NNodeTrackerServer::TNode* FindTabletLeaderNode(const TTablet* tablet) const;

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    TTablet* GetTabletOrThrow(TTabletId id);

    DECLARE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

    // COMPAT(savrus)
    NHydra::TEntityMap<TTabletCellBundle>& CompatTabletCellBundleMap();
    NHydra::TEntityMap<TTabletCell>& CompatTabletCellMap();

private:
    template <class TImpl>
    friend class NTableServer::TTableNodeTypeHandlerBase;
    friend class TTabletTypeHandler;
    friend class TTabletCellTypeHandler;
    friend class TTabletCellBundleTypeHandler;
    friend class TTableReplicaTypeHandler;
    friend class TTabletActionTypeHandler;
    friend class TTabletBalancer;
    class TImpl;

    void DestroyTable(NTableServer::TTableNode* table);

    void DestroyTablet(TTablet* tablet);

    TTabletCell* CreateTabletCell(TTabletCellBundle* cellBundle, NObjectClient::TObjectId hintId);
    void DestroyTabletCell(TTabletCell* cell);

    TTabletCellBundle* CreateTabletCellBundle(
        const TString& name,
        NObjectClient::TObjectId hintId,
        TTabletCellOptionsPtr options);
    void DestroyTabletCellBundle(TTabletCellBundle* cellBundle);

    TTableReplica* CreateTableReplica(
        NTableServer::TReplicatedTableNode* table,
        const TString& clusterName,
        const NYPath::TYPath& replicaPath,
        ETableReplicaMode mode,
        bool preserveTimestamps,
        NTransactionClient::EAtomicity atomicity,
        NTransactionClient::TTimestamp startReplicationTimestamp,
        const std::optional<std::vector<i64>>& startReplicationRowIndexes);
    void DestroyTableReplica(TTableReplica* replica);

    TTabletAction* CreateTabletAction(
        NObjectClient::TObjectId hintId,
        ETabletActionKind kind,
        const std::vector<TTablet*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const std::optional<int>& tabletCount,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime);

    void DestroyTabletAction(TTabletAction* action);

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
