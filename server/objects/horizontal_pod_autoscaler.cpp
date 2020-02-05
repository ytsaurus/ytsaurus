#include "horizontal_pod_autoscaler.h"
#include "replica_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<THorizontalPodAutoscaler, THorizontalPodAutoscaler::TSpec> THorizontalPodAutoscaler::SpecSchema{
    &HorizontalPodAutoscalersTable.Fields.Spec,
    [] (THorizontalPodAutoscaler* horizontalPodAutoscaler) { return &horizontalPodAutoscaler->Spec(); }
};

const TScalarAttributeSchema<THorizontalPodAutoscaler, THorizontalPodAutoscaler::TStatus> THorizontalPodAutoscaler::StatusSchema{
    &HorizontalPodAutoscalersTable.Fields.Status,
    [] (THorizontalPodAutoscaler* horizontalPodAutoscaler) { return &horizontalPodAutoscaler->Status(); }
};

////////////////////////////////////////////////////////////////////////////////

THorizontalPodAutoscaler::THorizontalPodAutoscaler(
    const TObjectId& id,
    const TObjectId& replicaSetId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, replicaSetId, typeHandler, session)
    , ReplicaSet_(this)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType THorizontalPodAutoscaler::GetType() const
{
    return EObjectType::HorizontalPodAutoscaler;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

