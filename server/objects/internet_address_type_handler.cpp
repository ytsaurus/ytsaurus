#include "virtual_service_type_handler.h"
#include "type_handler_detail.h"
#include "internet_address.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TInternetAddressTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TInternetAddressTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::InternetAddress)
    {
        SpecAttributeSchema_
            ->SetAttribute(TInternetAddress::SpecSchema);

        StatusAttributeSchema_
            ->SetAttribute(TInternetAddress::StatusSchema);
    }

    virtual const TDBTable* GetTable() override
    {
        return &InternetAddressesTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &InternetAddressesTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TInternetAddress(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateInternetAddressTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TInternetAddressTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

