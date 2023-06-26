#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/chunk_tree_statistics.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>
#include <yt/yt/server/lib/hydra_common/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/table_server/public.h>
#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/proto/table_ypath.pb.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletManager
    : public TRefCounted
{
public:
    explicit TTabletManager(NCellMaster::TBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();

    NYTree::IYPathServicePtr GetOrchidService();

    const ITabletChunkManagerPtr& GetTabletChunkManager() const;

    void PrepareMount(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze);
    void PrepareUnmount(
        TTabletOwnerBase* owner,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void PrepareRemount(
        TTabletOwnerBase* owner,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void PrepareFreeze(
        TTabletOwnerBase* owner,
        int firstTabletIndex,
        int lastTabletIndex);
    void PrepareUnfreeze(
        TTabletOwnerBase* owner,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void PrepareReshard(
        TTabletOwnerBase* owner,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        bool create = false);

    void ValidateMakeTableDynamic(NTableServer::TTableNode* table);
    void ValidateMakeTableStatic(NTableServer::TTableNode* table);

    void ValidateCloneTabletOwner(
        TTabletOwnerBase* sourceNode,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account);
    void ValidateBeginCopyTabletOwner(
        TTabletOwnerBase* sourceNode,
        NCypressServer::ENodeCloneMode mode);

    void Mount(
        TTabletOwnerBase* table,
        const TString& path,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId hintCellId,
        const std::vector<TTabletCellId>& targetCellIds,
        bool freeze,
        NTransactionClient::TTimestamp mountTimestamp);
    void Unmount(
        TTabletOwnerBase* table,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void Remount(
        TTabletOwnerBase* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void Freeze(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex);
    void Unfreeze(
        TTabletOwnerBase* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);
    void Reshard(
        TTabletOwnerBase* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys);

    void CloneTabletOwner(
        TTabletOwnerBase* sourceNode,
        TTabletOwnerBase* clonedNode,
        NCypressServer::ENodeCloneMode mode);

    void MakeTableDynamic(NTableServer::TTableNode* table);
    void MakeTableStatic(NTableServer::TTableNode* table);

    void AlterTableReplica(
        TTableReplica* replica,
        std::optional<bool> enabled,
        std::optional<ETableReplicaMode> mode,
        std::optional<NTransactionClient::EAtomicity> atomicity,
        std::optional<bool> preserveTimestamps,
        std::optional<bool> enableReplicatedTableTracker);

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

    void MergeTable(
        NTableServer::TTableNode* originatingNode,
        NTableServer::TTableNode* branchedNode);

    void OnNodeStorageParametersUpdated(NChunkServer::TChunkOwnerBase* node);

    TTabletCellBundle* FindTabletCellBundle(TTabletCellBundleId id);
    TTabletCellBundle* GetTabletCellBundleOrThrow(TTabletCellBundleId id);
    TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const TString& name, bool activeLifeStageOnly);
    TTabletCellBundle* GetDefaultTabletCellBundle();
    void SetTabletCellBundle(TTabletOwnerBase* table, TTabletCellBundle* cellBundle);

    TTabletCell* GetTabletCellOrThrow(TTabletCellId id);
    void ZombifyTabletCell(TTabletCell* cell);

    void DestroyTablet(TTabletBase* tablet);

    void DestroyTabletOwner(TTabletOwnerBase* table);

    NNodeTrackerServer::TNode* FindTabletLeaderNode(const TTabletBase* tablet) const;

    void UpdateExtraMountConfigKeys(std::vector<TString> keys);

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTabletBase);
    TTabletBase* GetTabletOrThrow(TTabletId id);

    DECLARE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

    void RecomputeTabletCellStatistics(NCellServer::TCellBase* cellBase);

    void OnHunkJournalChunkSealed(NChunkServer::TChunk* chunk);

    void AttachDynamicStoreToTablet(TTablet* tablet, NChunkServer::TDynamicStore* dynamicStore);

    // Backup stuff. Used internally by TBackupManager.
    void WrapWithBackupChunkViews(TTablet* tablet, NTransactionClient::TTimestamp maxClipTimestamp);
    TError PromoteFlushedDynamicStores(TTablet* tablet);
    TError ApplyBackupCutoff(TTablet* tablet);

    DECLARE_SIGNAL_WITH_ACCESSOR(void(TReplicatedTableData), ReplicatedTableCreated);
    DECLARE_SIGNAL_WITH_ACCESSOR(void(NTableClient::TTableId), ReplicatedTableDestroyed);
    DECLARE_SIGNAL_WITH_ACCESSOR(void(NTableClient::TTableId, NTabletClient::TReplicatedTableOptionsPtr), ReplicatedTableOptionsUpdated);

    DECLARE_SIGNAL(void(TReplicaData), ReplicaCreated);
    DECLARE_SIGNAL(void(NTabletClient::TTableReplicaId), ReplicaDestroyed);
    DECLARE_SIGNAL(void(NTabletClient::TTableReplicaId, ETableReplicaMode), ReplicaModeUpdated);
    DECLARE_SIGNAL(void(NTabletClient::TTableReplicaId, bool), ReplicaEnablementUpdated);
    DECLARE_SIGNAL_WITH_ACCESSOR(void(NTabletClient::TTableReplicaId, bool), ReplicaTrackingPolicyUpdated);

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
        bool enabled,
        NTransactionClient::TTimestamp startReplicationTimestamp,
        const std::optional<std::vector<i64>>& startReplicationRowIndexes);
    void ZombifyTableReplica(TTableReplica* replica);

    TTabletAction* CreateTabletAction(
        NObjectClient::TObjectId hintId,
        ETabletActionKind kind,
        const std::vector<TTabletBase*>& tablets,
        const std::vector<TTabletCell*>& cells,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::optional<int>& tabletCount,
        bool skipFreezing,
        TGuid correlationId,
        TInstant expirationTime,
        std::optional<TDuration> expirationTimeout);
    void DestroyTabletAction(TTabletAction* action);

    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
