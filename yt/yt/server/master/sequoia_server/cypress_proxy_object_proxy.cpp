#include "cypress_proxy_object_proxy.h"

#include "config.h"
#include "cypress_proxy_object.h"
#include "cypress_proxy_tracker.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/ytree/static_service_dispatcher.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NOrchid;
using namespace NRpc;
using namespace NServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyObjectProxy
    : public TNonversionedObjectProxyBase<TCypressProxyObject>
    , public virtual TStaticServiceDispatcher
{
public:
    TCypressProxyObjectProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TCypressProxyObject* proxy)
        : TNonversionedObjectProxyBase(bootstrap, metadata, proxy)
    {
        RegisterService(
            "orchid",
            BIND(&TCypressProxyObjectProxy::CreateOrchidService, Unretained(this)));
    }

private:
    using TBase = TNonversionedObjectProxyBase;

    IYPathServicePtr CreateOrchidService()
    {
        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
        const auto& config = Bootstrap_->GetDynamicConfig()->CypressProxyTracker;

        return CreateOrchidYPathService({
            .Channel = CreateRetryingChannel(
                New<TRetryingChannelConfig>(),
                cypressProxyTracker->GetCypressProxyChannelOrThrow(GetThisImpl()->GetAddress())),
            .Timeout = config->CypressProxyOrchidTimeout,
        });
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->emplace_back(EInternedAttributeKey::SequoiaReign);
        descriptors->emplace_back(EInternedAttributeKey::LastSeenTime);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* proxyObject = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::SequoiaReign:
                BuildYsonFluently(consumer)
                    .Value(proxyObject->GetSequoiaReign());
                return true;

            case EInternedAttributeKey::LastSeenTime:
                BuildYsonFluently(consumer)
                    .Value(proxyObject->GetLastSeenTime());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateCypressProxyObjectProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TCypressProxyObject* proxyObject)
{
    return New<TCypressProxyObjectProxy>(bootstrap, metadata, proxyObject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
