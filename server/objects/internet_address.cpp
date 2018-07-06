#include "internet_address.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TInternetAddress, TInternetAddress::TSpec> TInternetAddress::SpecSchema{
    &InternetAddressesTable.Fields.Spec,
    [] (TInternetAddress* ia) { return &ia->Spec(); }
};

const TScalarAttributeSchema<TInternetAddress, TInternetAddress::TStatus> TInternetAddress::StatusSchema{
    &InternetAddressesTable.Fields.Status,
    [] (TInternetAddress* ia) { return &ia->Status(); }
};

TInternetAddress::TInternetAddress(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TInternetAddress::GetType() const
{
    return EObjectType::InternetAddress;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

