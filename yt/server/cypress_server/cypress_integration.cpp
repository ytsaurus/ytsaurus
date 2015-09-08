#include "stdafx.h"
#include "cypress_integration.h"
#include "cypress_manager.h"
#include "virtual.h"

#include <server/cell_master/bootstrap.h>

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
