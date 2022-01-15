#include "host_proxy.h"

#include "host.h"
#include "node.h"
#include "node_tracker.h"
#include "rack.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class THostProxy
    : public TNonversionedObjectProxyBase<THost>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<THost>;

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);

        if (!GetThisImpl()->Nodes().empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove host with assigned nodes");
        }
    }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetMandatory(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Rack)
            .SetPresent(GetThisImpl()->GetRack())
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        auto* host = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(host->GetName());
                return true;

            case EInternedAttributeKey::Rack: {
                if (auto* rack = host->GetRack()) {
                    BuildYsonFluently(consumer)
                        .Value(rack->GetName());
                    return true;
                }

                break;
            }

            case EInternedAttributeKey::Nodes: {
                BuildYsonFluently(consumer)
                    .DoListFor(host->Nodes(), [] (TFluentList fluent, const TNode* node) {
                        fluent.Item().Value(node->GetDefaultAddress());
                    });
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* host = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::Rack: {
                auto rackName = ConvertTo<TString>(value);
                auto* rack = nodeTracker->GetRackByNameOrThrow(rackName);
                nodeTracker->SetHostRack(host, rack);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* host = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::Rack: {
                nodeTracker->SetHostRack(host, /*rack*/ nullptr);
                return true;
            }

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateHostProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    THost* host)
{
    return New<THostProxy>(bootstrap, metadata, host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
