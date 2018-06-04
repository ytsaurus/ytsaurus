#pragma once

#include "public.h"
#include "tablet_cell.h"
#include "tablet_cell_bundle.h"
#include "table_replica.h"
#include "tablet_action.h"
#include "tablet_action_type_handler.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/chunk_tree_statistics.h>

#include <yt/server/hydra/entity_map.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/object_server/public.h>

#include <yt/server/table_server/public.h>
#include <yt/server/table_server/table_node.h>

#include <yt/server/tablet_server/tablet_manager.pb.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

constexpr int TypicalTabletSlotCount = 10;
using TTabletCellSet = SmallVector<TTabletCell*, TypicalTabletSlotCount>;

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

    const TTabletCellSet* FindAssignedTabletCells(const TString& address) const;

    TTabletStatistics GetTabletStatistics(const TTablet* tablet);


    void MountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCell* hintCell,
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
        NTransactionServer::TTransaction* transaction,
        NCypressServer::ENodeCloneMode mode);

    void MakeTableDynamic(NTableServer::TTableNode* table);
    void MakeTableStatic(NTableServer::TTableNode* table);

    void SetTableReplicaEnabled(TTableReplica* replica, bool enabled);
    void SetTableReplicaMode(TTableReplica* replica, ETableReplicaMode mode);

    const TBundleNodeTrackerPtr& GetBundleNodeTracker();

    DECLARE_ENTITY_MAP_ACCESSORS(TabletCellBundle, TTabletCellBundle);
    TTabletCellBundle* FindTabletCellBundleByName(const TString& name);
    TTabletCellBundle* GetTabletCellBundleByNameOrThrow(const TString& name);
    void RenameTabletCellBundle(TTabletCellBundle* cellBundle, const TString& newName);
    void SetTabletCellBundleNodeTagFilter(TTabletCellBundle* bundle, const TString& formula);
    TTabletCellBundle* GetDefaultTabletCellBundle();
    void SetTabletCellBundle(NTableServer::TTableNode* table, TTabletCellBundle* cellBundle);

    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell);
    TTabletCell* GetTabletCellOrThrow(const TTabletCellId& id);

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    TTablet* GetTabletOrThrow(const TTabletId&);

    DECLARE_ENTITY_MAP_ACCESSORS(TableReplica, TTableReplica);
    DECLARE_ENTITY_MAP_ACCESSORS(TabletAction, TTabletAction);

    DECLARE_SIGNAL(void(TTabletCellBundle* bundle), TabletCellBundleCreated);
    DECLARE_SIGNAL(void(TTabletCellBundle* bundle), TabletCellBundleDestroyed);
    DECLARE_SIGNAL(void(TTabletCellBundle* bundle), TabletCellBundleNodeTagFilterChanged);

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

    TTabletCell* CreateTabletCell(TTabletCellBundle* cellBundle, const NObjectClient::TObjectId& hintId);
    void DestroyTabletCell(TTabletCell* cell);

    TTabletCellBundle* CreateTabletCellBundle(
        const TString& name,
        const NObjectClient::TObjectId& hintId,
        TTabletCellOptionsPtr options);
    void DestroyTabletCellBundle(TTabletCellBundle* cellBundle);

    TTableReplica* CreateTableReplica(
        NTableServer::TReplicatedTableNode* table,
        const TString& clusterName,
        const NYPath::TYPath& replicaPath,
        ETableReplicaMode mode,
        NTransactionClient::TTimestamp startReplicationTimestamp,
        const TNullable<std::vector<i64>>& startReplicationRowIndexes);
    void DestroyTableReplica(TTableReplica* replica);

    TTabletAction* CreateTabletAction(
        const NObjectClient::TObjectId& hintId,
        ETabletActionKind kind,
        const std::vector<TTablet*>& tabletIds,
        const std::vector<TTabletCell*>& cellIds,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const TNullable<int>& tabletCount,
        bool skipFreezing,
        const TNullable<bool>& freeze,
        bool preserve);

    void DestroyTabletAction(TTabletAction* action);

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
