#include "ip4_address_pool_type_handler.h"

#include "db_schema.h"
#include "ip4_address_pool.h"
#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TIP4AddressPoolsTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TIP4AddressPoolsTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::IP4AddressPool)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TIP4AddressPool::SpecSchema);

        StatusAttributeSchema_
            ->SetAttribute(TIP4AddressPool::StatusSchema);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TIP4AddressPool>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &IP4AddressPoolsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &IP4AddressPoolsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TIP4AddressPool(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateIP4AddressPoolsTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TIP4AddressPoolsTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

