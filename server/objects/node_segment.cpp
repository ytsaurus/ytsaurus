#include "node_segment.h"
#include "pod_set.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TNodeSegment, TNodeSegment::TSpec> TNodeSegment::SpecSchema{
    &NodeSegmentsTable.Fields.Spec,
    [] (TNodeSegment* segment) { return &segment->Spec(); }
};

const TOneToManyAttributeSchema<TNodeSegment, TPodSet> TNodeSegment::PodSetsSchema{
    &NodeSegmentToPodSetsTable,
    &NodeSegmentToPodSetsTable.Fields.NodeSegmentId,
    &NodeSegmentToPodSetsTable.Fields.PodSetId,
    [] (TNodeSegment* segment) { return &segment->PodSets(); },
    [] (TPodSet* podSet) { return &podSet->Spec().NodeSegment(); },
};

TNodeSegment::TNodeSegment(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
    , PodSets_(this, &PodSetsSchema)
{ }

EObjectType TNodeSegment::GetType() const
{
    return EObjectType::NodeSegment;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

