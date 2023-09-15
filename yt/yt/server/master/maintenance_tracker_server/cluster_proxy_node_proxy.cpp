#include "cluster_proxy_node_proxy.h"

#include "cluster_proxy_node.h"
#include "helpers.h"

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NMaintenanceTrackerServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TClusterProxyNodeProxy
    : public TMapNodeProxy
{
public:
    using TMapNodeProxy::TMapNodeProxy;

private:
    using TBase = TMapNodeProxy;

    TClusterProxyNode* GetThisImpl()
    {
        return TNontemplateCypressNodeProxyBase::GetThisImpl<TClusterProxyNode>();
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->emplace_back(EInternedAttributeKey::Banned)
            .SetWritable(true);
        descriptors->push_back(EInternedAttributeKey::MaintenanceRequests);
    }

    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* impl = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Banned:
                BuildYsonFluently(consumer)
                    .Value(impl->IsBanned());
                return true;

            case EInternedAttributeKey::MaintenanceRequests:
                SerializeMaintenanceRequestsOf(impl, consumer);
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* impl = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Banned: {
                // COMPAT(kvk1920): This attribute will be made non-writable in favor of add_maintenance command.
                if (ConvertTo<bool>(value)) {
                    const auto& securityManager = Bootstrap_->GetSecurityManager();
                    auto* user = securityManager->GetAuthenticatedUser();
                    auto* hydraContext = NHydra::GetCurrentHydraContext();

                    Y_UNUSED(impl->SetMaintenanceFlag(
                        EMaintenanceType::Ban,
                        user ? user->GetName() : "",
                        hydraContext->GetTimestamp()));
                } else {
                    Y_UNUSED(impl->ClearMaintenanceFlag(EMaintenanceType::Ban));
                }
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateClusterProxyNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TClusterProxyNode* trunkNode)
{
    return New<TClusterProxyNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer
