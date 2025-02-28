#include "cypress_integration.h"

#include "cypress_proxy_object.h"
#include "cypress_proxy_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
        auto* proxyObject = cypressProxyTracker->FindCypressProxyByAddress(key);
        if (!IsObjectAlive(proxyObject)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(proxyObject);
    }

    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
        const auto& proxies = cypressProxyTracker->CypressProxies();

        std::vector<std::string> keys;
        keys.reserve(std::min(limit, std::ssize(proxies)));
        for (auto [proxyId, proxy] : proxies) {
            if (std::ssize(keys) >= limit) {
                break;
            }
            if (!IsObjectAlive(proxy)) {
                continue;
            }
            keys.push_back(proxy->GetAddress());
        }
        return keys;
    }

    i64 GetSize() const override
    {
        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
        return cypressProxyTracker->CypressProxies().GetSize();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateCypressProxyMapTypeHandler(TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::CypressProxyMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TCypressProxyMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
