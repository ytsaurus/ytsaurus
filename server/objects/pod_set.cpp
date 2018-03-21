#include "pod_set.h"
#include "pod.h"
#include "node_segment.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

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

TPodSet::TSpec::TSpec(TPodSet* podSet)
    : AntiaffinityConstraints_(podSet, &AntiaffinityConstraintsSchema)
    , NodeSegment_(podSet, &NodeSegmentSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

TPodSet::TPodSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Pods_(this)
    , Spec_(this)
{ }

EObjectType TPodSet::GetType() const
{
    return EObjectType::PodSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

