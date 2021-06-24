#include "cypress_integration.h"

#include "orchid_ypath_service.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/node.h>
#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

namespace NYT::NOrchid {

using namespace NCypressServer;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::Orchid,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return CreateOrchidYPathService(bootstrap->GetNodeChannelFactory(), std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid
