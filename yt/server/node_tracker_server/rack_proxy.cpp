#include "stdafx.h"
#include "rack_proxy.h"
#include "rack.h"
#include "node.h"
#include "node_tracker.h"

#include <core/ytree/fluent.h>

#include <server/object_server/object_detail.h>

#include <cell_master/bootstrap.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TRackProxy
    : public TNonversionedObjectProxyBase<TRack>
{
public:
    TRackProxy(NCellMaster::TBootstrap* bootstrap, TRack* rack)
        : TBase(bootstrap, rack)
    { }

private:
    typedef TNonversionedObjectProxyBase<TRack> TBase;

    virtual void ValidateRemoval() override
    { }

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor("name")
            .SetReplicated(true));
        descriptors->push_back("index");
        descriptors->push_back("nodes");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        const auto* rack = GetThisTypedImpl();

        if (key == "name") {
            BuildYsonFluently(consumer)
                .Value(rack->GetName());
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

    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        auto* rack = GetThisTypedImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "name") {
            auto newName = ConvertTo<Stroka>(value);
            nodeTracker->RenameRack(rack, newName);
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

};

IObjectProxyPtr CreateRackProxy(
    NCellMaster::TBootstrap* bootstrap,
    TRack* rack)
{
    return New<TRackProxy>(bootstrap, rack);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT

