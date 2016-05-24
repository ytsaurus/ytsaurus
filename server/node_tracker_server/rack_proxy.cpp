#include "rack_proxy.h"
#include "node_tracker.h"
#include "rack.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/core/misc/common.h>

#include <yt/core/ytree/fluent.h>

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

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeInfo>* attributes) override
    {
        attributes->push_back("name");
        attributes->push_back("index");
        attributes->push_back("nodes");
        TBase::ListSystemAttributes(attributes);
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
            BuildYsonFluently(consumer)
                .Value(nodeTracker->GetNodeAddressesByRack(rack));
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

