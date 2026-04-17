#include "cypress_integration.h"

#include "cypress_proxy_object.h"
#include "cypress_proxy_tracker.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/lib/cypress_proxy/config.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NCypressProxy;
using namespace NObjectServer;
using namespace NServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    using TBase = TVirtualSinglecellMapBase;

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

    // ISystemAttributeProvider overrides
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Config)
            .SetWritable(true)
            .SetOpaque(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {

        switch (key) {
            case EInternedAttributeKey::Config:
            {
                const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
                BuildYsonFluently(consumer)
                    .Value(cypressProxyTracker->GetDynamicConfig());
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        switch (key) {
            case EInternedAttributeKey::Config: {
                ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

                auto config = ConvertTo<TCypressProxyDynamicConfigPtr>(value);
                const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
                cypressProxyTracker->SetDynamicConfig(std::move(config));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
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
