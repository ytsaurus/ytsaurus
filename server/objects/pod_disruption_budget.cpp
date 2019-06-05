#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "db_schema.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPodDisruptionBudget, TPodDisruptionBudget::TStatus> TPodDisruptionBudget::StatusSchema{
    &PodDisruptionBudgetsTable.Fields.Status,
    [] (TPodDisruptionBudget* podDisruptionBudget) { return &podDisruptionBudget->Status(); }
};

const TScalarAttributeSchema<TPodDisruptionBudget, TPodDisruptionBudget::TSpec> TPodDisruptionBudget::SpecSchema{
    &PodDisruptionBudgetsTable.Fields.Spec,
    [] (TPodDisruptionBudget* podDisruptionBudget) { return &podDisruptionBudget->Spec(); }
};

const TOneToManyAttributeSchema<TPodDisruptionBudget, TPodSet> TPodDisruptionBudget::PodSetsSchema{
    &PodDisruptionBudgetToPodSetsTable,
    &PodDisruptionBudgetToPodSetsTable.Fields.PodDisruptionBudgetId,
    &PodDisruptionBudgetToPodSetsTable.Fields.PodSetId,
    [] (TPodDisruptionBudget* podDisruptionBudget) { return &podDisruptionBudget->PodSets(); },
    [] (TPodSet* podSet) { return &podSet->Spec().PodDisruptionBudget(); }
};

const TTimestampAttributeSchema TPodDisruptionBudget::StatusUpdateTimestampSchema{
    &PodDisruptionBudgetsTable.Fields.StatusUpdateTag
};

////////////////////////////////////////////////////////////////////////////////

TPodDisruptionBudget::TPodDisruptionBudget(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Status_(this, &StatusSchema)
    , Spec_(this, &SpecSchema)
    , PodSets_(this, &PodSetsSchema)
    , StatusUpdateTimestamp_(this, &StatusUpdateTimestampSchema)
{ }

EObjectType TPodDisruptionBudget::GetType() const
{
    return EObjectType::PodDisruptionBudget;
}

////////////////////////////////////////////////////////////////////////////////

void TPodDisruptionBudget::FreezeUntilSync(const TString& message)
{
    UpdateStatus(0, message);
}

void TPodDisruptionBudget::UpdateStatus(i32 allowedPodDisruptions, const TString& message)
{
    Status()->set_allowed_pod_disruptions(allowedPodDisruptions);
    *Status()->mutable_last_update_time() = NYT::GetProtoNow();
    Status()->set_last_update_message(message);
    StatusUpdateTimestamp().Touch();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

