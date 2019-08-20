#include "ip4_address_pool.h"

#include "db_schema.h"
#include "internet_address.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TIP4AddressPool, TIP4AddressPool::TSpec> TIP4AddressPool::SpecSchema{
    &IP4AddressPoolsTable.Fields.Spec,
    [] (TIP4AddressPool* pool) { return &pool->Spec(); }
};

const TScalarAttributeSchema<TIP4AddressPool, TIP4AddressPool::TStatus> TIP4AddressPool::StatusSchema{
    &IP4AddressPoolsTable.Fields.Status,
    [] (TIP4AddressPool* pool) { return &pool->Status(); }
};

TIP4AddressPool::TIP4AddressPool(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , InternetAddresses_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TIP4AddressPool::GetType() const
{
    return EObjectType::IP4AddressPool;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

