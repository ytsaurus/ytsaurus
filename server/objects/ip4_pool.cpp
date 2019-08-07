#include "ip4_pool.h"

#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TIP4Pool, TIP4Pool::TSpec> TIP4Pool::SpecSchema{
    &InternetAddressesTable.Fields.Spec,
    [] (TIP4Pool* pool) { return &pool->Spec(); }
};

const TScalarAttributeSchema<TIP4Pool, TIP4Pool::TStatus> TIP4Pool::StatusSchema{
    &InternetAddressesTable.Fields.Status,
    [] (TIP4Pool* pool) { return &pool->Status(); }
};

TIP4Pool::TIP4Pool(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TIP4Pool::GetType() const
{
    return EObjectType::IP4Pool;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

