#include "endpoint_type_handler.h"
#include "type_handler_detail.h"
#include "endpoint.h"
#include "endpoint_set.h"
#include "db_schema.h"

#include <yp/server/scheduler/resource_manager.h>

#include <yp/server/net/net_manager.h>

#include <yp/server/master/bootstrap.h>

#include <yt/core/net/address.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NAccessControl;

using namespace NYT::NNet;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TEndpointTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TEndpointTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Endpoint)
    {
        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("endpoint_set_id")
                    ->SetParentAttribute()
                    ->SetMandatory()
            });

        SpecAttributeSchema_
            ->SetAttribute(TEndpoint::SpecSchema)
            ->SetValidator<TEndpoint>(std::bind(&TEndpointTypeHandler::ValidateSpec, this, _1, _2));
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::EndpointSet;
    }

    virtual const TDBField* GetIdField() override
    {
        return &EndpointsTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &EndpointsTable.Fields.Meta_EndpointSetId;
    }

    virtual const TDBTable* GetTable() override
    {
        return &EndpointsTable;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TEndpointSet>()->Endpoints();
    }

    virtual TObject* GetAccessControlParent(TObject* object) override
    {
        return object->As<TEndpoint>()->EndpointSet().Load();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TEndpoint>(id, parentId, this, session);
    }

private:
    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }

    void ValidateSpec(const TTransactionPtr& transaction, TEndpoint* endpoint)
    {
        const auto& spec = endpoint->Spec().Load();
        if (spec.has_ip6_address()) {
            TIP6Address::FromString(spec.ip6_address());
        }
        // TODO(babenko): validate FQDN?
        // TODO(babenko): validate IP4?
    }
};

std::unique_ptr<IObjectTypeHandler> CreateEndpointTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TEndpointTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

