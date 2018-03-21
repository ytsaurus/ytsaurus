#include "endpoint.h"
#include "endpoint_set.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TEndpoint, TEndpoint::TSpec> TEndpoint::SpecSchema{
    &EndpointsTable.Fields.Spec,
    [] (TEndpoint* endpoint) { return &endpoint->Spec(); }
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
{ }

EObjectType TEndpoint::GetType() const
{
    return EObjectType::Endpoint;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

