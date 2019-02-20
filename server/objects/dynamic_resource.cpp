#include "dynamic_resource.h"
#include "pod_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TDynamicResource, TDynamicResource::TSpec> TDynamicResource::SpecSchema{
    &DynamicResourcesTable.Fields.Spec,
    [] (TDynamicResource* dynamicResource) { return &dynamicResource->Spec(); }
};

const TScalarAttributeSchema<TDynamicResource, TDynamicResource::TStatus> TDynamicResource::StatusSchema{
    &DynamicResourcesTable.Fields.Status,
    [] (TDynamicResource* dynamicResource) { return &dynamicResource->Status(); }
};


////////////////////////////////////////////////////////////////////////////////

TDynamicResource::TDynamicResource(
    const TObjectId& id,
    const TObjectId& podSetId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, podSetId, typeHandler, session)
    , PodSet_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TDynamicResource::GetType() const
{
    return EObjectType::DynamicResource;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

