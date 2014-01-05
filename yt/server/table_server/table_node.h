#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <ytlib/new_table_client/public.h>

#include <server/chunk_server/chunk_owner_base.h>

#include <server/cypress_server/node_detail.h>

#include <server/tablet_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNode
    : public NChunkServer::TChunkOwnerBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Sorted);
    DEFINE_BYREF_RW_PROPERTY(NVersionedTableClient::TKeyColumns, KeyColumns);

    // Most tables have no tablets.
    static const int MaxTabletCount = 1000;
    typedef std::vector<NTabletServer::TTablet*> TTabletList;
    DEFINE_BYREF_RW_PROPERTY(TTabletList, Tablets);

public:
    explicit TTableNode(const NCypressServer::TVersionedNodeId& id);

    virtual NObjectClient::EObjectType GetObjectType() const;
    TTableNode* GetTrunkNode() const;

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateTableTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

