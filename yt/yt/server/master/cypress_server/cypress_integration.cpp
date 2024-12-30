#include "cypress_integration.h"
#include "cypress_manager.h"
#include "private.h"
#include "virtual.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualLockMap
    : public TVirtualMulticellMapBase
{
public:
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 limit) const override
    {
        return MakeFuture(NYT::GetKeys(Locks(), limit));
    }

    bool IsValid(TObject* object) const override
    {
        return IsObjectAlive(object);
    }

    bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

    TFuture<i64> GetSize() const override
    {
        return MakeFuture<i64>(Locks().size());
    }

    NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/locks";
    }

    const TReadOnlyEntityMap<TLock>& Locks() const
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->Locks();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateLockMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LockMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualLockMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualAccessControlObjectNamespaceMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        std::vector<std::string> keys;
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        for (auto [id, node] : cypressManager->AccessControlObjectNamespaces()) {
            if (ssize(keys) >= limit) {
                break;
            }
            if (!IsObjectAlive(node)) {
                continue;
            }
            keys.push_back(node->GetName());
        }
        return keys;
    }

    i64 GetSize() const override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->GetAccessControlObjectNamespaceCount();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        auto* node = cypressManager->FindAccessControlObjectNamespaceByName(key);

        if (!IsObjectAlive(node)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(node);
    }
};

INodeTypeHandlerPtr CreateAccessControlObjectNamespaceMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::AccessControlObjectNamespaceMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualAccessControlObjectNamespaceMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
