#include "rack_proxy.h"
#include "node_tracker.h"
#include "data_center.h"
#include "rack.h"
#include "node.h"

#include <yt/server/cell_master/bootstrap.h>

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

        descriptors->push_back(TAttributeDescriptor("name")
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor("data_center")
            .SetPresent(GetThisImpl()->GetDataCenter())
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back("index");
        descriptors->push_back("nodes");
    }

    virtual bool GetBuiltinAttribute(const TString& key, NYson::IYsonConsumer* consumer) override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto* rack = GetThisImpl();

        if (key == "name") {
            BuildYsonFluently(consumer)
                .Value(rack->GetName());
            return true;
        }

        if (key == "data_center" && rack->GetDataCenter()) {
            BuildYsonFluently(consumer)
                .Value(rack->GetDataCenter()->GetName());
            return true;
        }

        if (key == "index") {
            BuildYsonFluently(consumer)
                .Value(rack->GetIndex());
            return true;
        }

        if (key == "nodes") {
            auto nodes = nodeTracker->GetRackNodes(rack);
            BuildYsonFluently(consumer)
                .DoListFor(nodes, [] (TFluentList fluent, const TNode* node) {
                    fluent.Item().Value(node->GetDefaultAddress());
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        auto* rack = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "name") {
            auto newName = ConvertTo<TString>(value);
            nodeTracker->RenameRack(rack, newName);
            return true;
        }

        if (key == "data_center") {
            auto dcName = ConvertTo<TString>(value);
            auto* dc = nodeTracker->GetDataCenterByNameOrThrow(dcName);
            nodeTracker->SetRackDataCenter(rack, dc);
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual bool RemoveBuiltinAttribute(const TString& key) override {
        auto* rack = GetThisImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "data_center") {
            nodeTracker->SetRackDataCenter(rack, nullptr);
            return true;
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

