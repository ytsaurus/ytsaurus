#include "stdafx.h"
#include "cypress_integration.h"
#include "cypress_manager.h"
#include "virtual.h"

#include <core/misc/string.h>

#include <core/ytree/virtual.h>

#include <server/object_server/object_manager.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TVirtualLockMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualLockMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        return ConvertToStrings(ToObjectIds(cypressManager->GetLocks(sizeLimit)));
    }

    virtual size_t GetSize() const override
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        return cypressManager->GetLockCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TTransactionId::FromString(key);

        auto cypressManager = Bootstrap->GetCypressManager();
        auto* lock = cypressManager->FindLock(id);
        if (!IsObjectAlive(lock)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(lock);
    }
};

INodeTypeHandlerPtr CreateLockMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualLockMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LockMap,
        service,
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
