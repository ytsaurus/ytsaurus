#include "cypress_integration.h"
#include "cypress_manager.h"
#include "virtual.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateLockMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = CreateVirtualObjectMap(
        bootstrap,
        bootstrap->GetCypressManager()->Locks());
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LockMap,
        service,
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
