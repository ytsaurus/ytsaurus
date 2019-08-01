#include "endpoint_set.h"
#include "endpoint.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TEndpointSet, TEndpointSet::TSpec> TEndpointSet::SpecSchema{
    &EndpointSetsTable.Fields.Spec,
    [] (TEndpointSet* endpointSet) { return &endpointSet->Spec(); }
};

const TTimestampAttributeSchema TEndpointSet::TStatus::LastEndpointsUpdateTimestampSchema{
    &EndpointSetsTable.Fields.Status_LastEndpointsUpdateTag
};

TEndpointSet::TStatus::TStatus(TEndpointSet* endpointSet)
    : LastEndpointsUpdateTimestamp_(endpointSet, &LastEndpointsUpdateTimestampSchema)
{ }

TEndpointSet::TEndpointSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Endpoints_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this)
{ }

EObjectType TEndpointSet::GetType() const
{
    return EObjectType::EndpointSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

