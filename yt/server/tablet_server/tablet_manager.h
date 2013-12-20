#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

#include <server/hydra/entity_map.h>
#include <server/hydra/mutation.h>

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

#include <server/table_server/public.h>

#include <server/tablet_server/tablet_manager.pb.h>

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

    NHydra::TMutationPtr CreateStartSlotsMutation(
        const NProto::TReqStartSlots& request);

    NHydra::TMutationPtr CreateSetCellStateMutation(
        const NProto::TReqSetCellState& request);

    NHydra::TMutationPtr CreateRevokePeerMutation(
        const NProto::TReqRevokePeer& request);

    NVersionedTableClient::TTableSchema GetTableSchema(NTableServer::TTableNode* table);

    void MountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);

    void UnmountTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex = -1,
        int lastTabletIndex = -1);

    void ForceUnmountTable(NTableServer::TTableNode* table);

    void ReshardTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        const std::vector<NVersionedTableClient::TOwningKey>& pivotKeys);

    DECLARE_ENTITY_MAP_ACCESSORS(TabletCell, TTabletCell, TTabletCellId);
    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet, TTabletId);

private:
    class TTabletCellTypeHandler;
    class TTabletTypeHandler;
    class TImpl;
    
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
