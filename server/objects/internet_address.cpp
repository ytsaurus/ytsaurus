#include "internet_address.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

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
    const TObjectId& ip4AddressPoolId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, ip4AddressPoolId, typeHandler, session)
    , IP4AddressPool_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TInternetAddress::GetType() const
{
    return EObjectType::InternetAddress;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

