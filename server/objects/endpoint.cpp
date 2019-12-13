#include "endpoint.h"
#include "endpoint_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TEndpoint, TEndpoint::TSpec> TEndpoint::SpecSchema{
    &EndpointsTable.Fields.Spec,
    [] (TEndpoint* endpoint) { return &endpoint->Spec(); }
};

const TScalarAttributeSchema<TEndpoint, TEndpoint::TStatus> TEndpoint::StatusSchema{
    &EndpointsTable.Fields.Status,
    [] (TEndpoint* endpoint) { return &endpoint->Status(); }
};

////////////////////////////////////////////////////////////////////////////////

TEndpoint::TEndpoint(
    const TObjectId& id,
    const TObjectId& endpointSetId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, endpointSetId, typeHandler, session)
    , EndpointSet_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TEndpoint::GetType() const
{
    return EObjectType::Endpoint;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

