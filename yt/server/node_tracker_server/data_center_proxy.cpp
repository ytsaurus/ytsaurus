#include "data_center_proxy.h"
#include "data_center.h"
#include "node.h"
#include "node_tracker.h"
#include "rack.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NNodeTrackerServer {

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

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(EInternedAttributeKey::Racks);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        const auto* dc = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(dc->GetName());
                return true;

            case EInternedAttributeKey::Racks: {
                auto racks = nodeTracker->GetDataCenterRacks(dc);
                BuildYsonFluently(consumer)
                    .DoListFor(racks, [] (TFluentList fluent, const TRack* rack) {
                        fluent.Item().Value(rack->GetName());
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
        auto* dc = GetThisImpl();
        auto nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::Name: {
                auto newName = ConvertTo<TString>(value);
                nodeTracker->RenameDataCenter(dc, newName);
                return true;
            }

            default:
                break;
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

} // namespace NYT::NNodeTrackerServer
