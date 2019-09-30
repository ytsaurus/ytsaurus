#include "internet_address_type_handler.h"

#include "db_schema.h"
#include "internet_address.h"
#include "ip4_address_pool.h"
#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TInternetAddressTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TInternetAddressTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::InternetAddress)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("ip4_address_pool_id")
                    ->SetParentIdAttribute()
                    ->SetMandatory()
            });

        SpecAttributeSchema_
            ->SetAttribute(TInternetAddress::SpecSchema);

        StatusAttributeSchema_
            ->SetAttribute(TInternetAddress::StatusSchema);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TInternetAddress>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &InternetAddressesTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &InternetAddressesTable.Fields.Meta_Id;
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::IP4AddressPool;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<TInternetAddress>()->IP4AddressPool().Load();
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TIP4AddressPool>()->InternetAddresses();
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &InternetAddressesTable.Fields.Meta_IP4AddressPoolId;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::unique_ptr<TObject>(new TInternetAddress(id, parentId, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateInternetAddressTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TInternetAddressTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

