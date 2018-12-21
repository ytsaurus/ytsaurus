#include "endpoint_set_type_handler.h"
#include "type_handler_detail.h"
#include "endpoint_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TEndpointSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TEndpointSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::EndpointSet)
    {
        SpecAttributeSchema_
            ->SetAttribute(TEndpointSet::SpecSchema);

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TEndpointSet>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &EndpointSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &EndpointSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TEndpointSet(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateEndpointSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TEndpointSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

