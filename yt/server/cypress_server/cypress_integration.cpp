#include "cypress_integration.h"
#include "cypress_manager.h"
#include "virtual.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateLockMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LockMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return CreateVirtualObjectMap(
                bootstrap,
                bootstrap->GetCypressManager()->Locks(),
                owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
