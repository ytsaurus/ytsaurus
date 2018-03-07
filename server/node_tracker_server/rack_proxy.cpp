#include "rack_proxy.h"
#include "node_tracker.h"
#include "data_center.h"
#include "rack.h"
#include "node.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/object_server/interned_attributes.h>
#include <yt/server/object_server/object_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TRackProxy
    : public TNonversionedObjectProxyBase<TRack>
{
public:
    TRackProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TRack* rack)
        : TBase(bootstrap, metadata, rack)
    { }

private:
    typedef TNonversionedObjectProxyBase<TRack> TBase;

    virtual void ValidateRemoval() override
    { }

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DataCenter)
            .SetPresent(GetThisImpl()->GetDataCenter())
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back(EInternedAttributeKey::Index);
        descriptors->push_back(EInternedAttributeKey::Nodes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto* rack = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(rack->GetName());
                return true;

            case EInternedAttributeKey::DataCenter:
                if (!rack->GetDataCenter()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(rack->GetDataCenter()->GetName());
                return true;

            case EInternedAttributeKey::Index:
                BuildYsonFluently(consumer)
                    .Value(rack->GetIndex());
                return true;

            case EInternedAttributeKey::Nodes: {
                auto nodes = nodeTracker->GetRackNodes(rack);
                BuildYsonFluently(consumer)
                    .DoListFor(nodes, [] (TFluentList fluent, const TNode* node) {
                        fluent.Item().Value(node->GetDefaultAddress());
                    });
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* rack = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::Name: {
                auto newName = ConvertTo<TString>(value);
                nodeTracker->RenameRack(rack, newName);
                return true;
            }

            case EInternedAttributeKey::DataCenter: {
                auto dcName = ConvertTo<TString>(value);
                auto* dc = nodeTracker->GetDataCenterByNameOrThrow(dcName);
                nodeTracker->SetRackDataCenter(rack, dc);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual bool RemoveBuiltinAttribute(TInternedAttributeKey key) override {
        auto* rack = GetThisImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::DataCenter:
                nodeTracker->SetRackDataCenter(rack, nullptr);
                return true;

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }
};

IObjectProxyPtr CreateRackProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TRack* rack)
{
    return New<TRackProxy>(bootstrap, metadata, rack);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

