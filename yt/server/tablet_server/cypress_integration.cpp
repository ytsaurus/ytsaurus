#include "stdafx.h"
#include "cypress_integration.h"
#include "tablet_cell.h"
#include "tablet.h"

#include <server/cell_master/bootstrap.h>

#include <server/cypress_server/virtual.h>

#include <server/tablet_server/tablet_manager.h>

namespace NYT {
namespace NTabletServer {

using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateTabletCellMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = CreateVirtualObjectMap(
        bootstrap,
        bootstrap->GetTabletManager()->TabletCells());
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletCellMap,
        service,
        EVirtualNodeOptions::RedirectSelf);
}

INodeTypeHandlerPtr CreateTabletMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = CreateVirtualObjectMap(
        bootstrap,
        bootstrap->GetTabletManager()->Tablets());
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::TabletMap,
        service,
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
