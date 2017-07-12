#include "data_center_proxy.h"
#include "data_center.h"
#include "node.h"
#include "node_tracker.h"
#include "rack.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TDataCenterProxy
    : public TNonversionedObjectProxyBase<TDataCenter>
{
public:
    TDataCenterProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TDataCenter* dc)
        : TBase(bootstrap, metadata, dc)
    { }

private:
    typedef TNonversionedObjectProxyBase<TDataCenter> TBase;

    virtual void ValidateRemoval() override
    { }

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor("name")
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back("racks");
    }

    virtual bool GetBuiltinAttribute(const TString& key, NYson::IYsonConsumer* consumer) override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        const auto* dc = GetThisImpl();

        if (key == "name") {
            BuildYsonFluently(consumer)
                .Value(dc->GetName());
            return true;
        }

        if (key == "racks") {
            auto racks = nodeTracker->GetDataCenterRacks(dc);
            BuildYsonFluently(consumer)
                .DoListFor(racks, [] (TFluentList fluent, const TRack* rack) {
                    fluent.Item().Value(rack->GetName());
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const TString& key, const TYsonString& value) override
    {
        auto* dc = GetThisImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

        if (key == "name") {
            auto newName = ConvertTo<TString>(value);
            nodeTracker->RenameDataCenter(dc, newName);
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

IObjectProxyPtr CreateDataCenterProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TDataCenter* dc)
{
    return New<TDataCenterProxy>(bootstrap, metadata, dc);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
