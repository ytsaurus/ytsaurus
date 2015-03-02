#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

#include <server/hydra/entity_map.h>
#include <server/hydra/mutation.h>

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

#include <server/table_server/public.h>

#include <server/tablet_server/tablet_manager.pb.h>

#include <server/chunk_server/chunk_tree_statistics.h>

namespace NYT {
namespace NTabletServer {

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

    int GetAssignedTabletCellCount(const Stroka& address) const;

    NVersionedTableClient::TTableSchema GetTableSchema(NTableServer::TTableNode* table);

    TTabletStatistics GetTabletStatistics(const TTablet* tablet);


    void MountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        TTabletCellId cellId);

    void UnmountTable(
        NTableServer::TTableNode* table,
        bool force,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);

    void RemountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);

    void ClearTablets(
        NTableServer::TTableNode* table);

    void ReshardTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        const std::vector<NVersionedTableClient::TOwningKey>& pivotKeys);



    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell, TTabletCellId);
    TTabletCell* GetTabletCellOrThrow(const TTabletCellId& id);

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    class TTabletCellTypeHandler;
    class TTabletTypeHandler;
    class TImpl;
    
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
