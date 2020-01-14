#include "pod_set.h"
#include "pod.h"
#include "dynamic_resource.h"
#include "resource_cache.h"
#include "node_segment.h"
#include "account.h"
#include "pod_disruption_budget.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPodSet, TPodSet::TSpec::TAntiaffinityConstraints> TPodSet::TSpec::AntiaffinityConstraintsSchema{
    &PodSetsTable.Fields.Spec_AntiaffinityConstraints,
    [] (TPodSet* podSet) { return &podSet->Spec().AntiaffinityConstraints(); }
};

const TManyToOneAttributeSchema<TPodSet, TNodeSegment> TPodSet::TSpec::NodeSegmentSchema{
    &PodSetsTable.Fields.Spec_NodeSegmentId,
    [] (TPodSet* podSet) { return &podSet->Spec().NodeSegment(); },
    [] (TNodeSegment* segment) { return &segment->PodSets(); }
};

const TManyToOneAttributeSchema<TPodSet, TAccount> TPodSet::TSpec::AccountSchema{
    &PodSetsTable.Fields.Spec_AccountId,
    [] (TPodSet* podSet) { return &podSet->Spec().Account(); },
    [] (TAccount* account) { return &account->PodSets(); }
};

const TManyToOneAttributeSchema<TPodSet, TPodDisruptionBudget> TPodSet::TSpec::PodDisruptionBudgetSchema{
    &PodSetsTable.Fields.Spec_PodDisruptionBudgetId,
    [] (TPodSet* podSet) { return &podSet->Spec().PodDisruptionBudget(); },
    [] (TPodDisruptionBudget* podDisruptionBudget) { return &podDisruptionBudget->PodSets(); }
};

const TScalarAttributeSchema<TPodSet, TString> TPodSet::TSpec::NodeFilterSchema{
    &PodSetsTable.Fields.Spec_NodeFilter,
    [] (TPodSet* podSet) { return &podSet->Spec().NodeFilter(); }
};

const TScalarAttributeSchema<TPodSet, TPodSet::TSpec::TEtc> TPodSet::TSpec::EtcSchema{
    &PodSetsTable.Fields.Spec_Etc,
    [] (TPodSet* podSet) { return &podSet->Spec().Etc(); }
};

TPodSet::TSpec::TSpec(TPodSet* podSet)
    : AntiaffinityConstraints_(podSet, &AntiaffinityConstraintsSchema)
    , NodeSegment_(podSet, &NodeSegmentSchema)
    , Account_(podSet, &AccountSchema)
    , PodDisruptionBudget_(podSet, &PodDisruptionBudgetSchema)
    , NodeFilter_(podSet, &NodeFilterSchema)
    , Etc_(podSet, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

TPodSet::TPodSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Pods_(this)
    , DynamicResources_(this)
    , ResourceCache_(this)
    , Spec_(this)
{ }

EObjectType TPodSet::GetType() const
{
    return EObjectType::PodSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

