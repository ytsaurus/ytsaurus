#include "node_segment.h"
#include "pod_set.h"
#include "replica_set.h"
#include "multi_cluster_replica_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TNodeSegment, TNodeSegment::TSpec> TNodeSegment::SpecSchema{
    &NodeSegmentsTable.Fields.Spec,
    [] (TNodeSegment* segment) { return &segment->Spec(); }
};

const TScalarAttributeSchema<TNodeSegment, TNodeSegment::TStatus> TNodeSegment::StatusSchema{
    &NodeSegmentsTable.Fields.Status,
    [] (TNodeSegment* segment) { return &segment->Status(); }
};

const TOneToManyAttributeSchema<TNodeSegment, TPodSet> TNodeSegment::PodSetsSchema{
    &NodeSegmentToPodSetsTable,
    &NodeSegmentToPodSetsTable.Fields.NodeSegmentId,
    &NodeSegmentToPodSetsTable.Fields.PodSetId,
    [] (TNodeSegment* segment) { return &segment->PodSets(); },
    [] (TPodSet* podSet) { return &podSet->Spec().NodeSegment(); },
};

const TOneToManyAttributeSchema<TNodeSegment, TReplicaSet> TNodeSegment::ReplicaSetsSchema{
    &NodeSegmentToReplicaSetsTable,
    &NodeSegmentToReplicaSetsTable.Fields.NodeSegmentId,
    &NodeSegmentToReplicaSetsTable.Fields.ReplicaSetId,
    [] (TNodeSegment* segment) { return &segment->ReplicaSets(); },
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().NodeSegment(); },
};

const TOneToManyAttributeSchema<TNodeSegment, TMultiClusterReplicaSet> TNodeSegment::MultiClusterReplicaSetsSchema{
    &NodeSegmentToMultiClusterReplicaSetsTable,
    &NodeSegmentToMultiClusterReplicaSetsTable.Fields.NodeSegmentId,
    &NodeSegmentToMultiClusterReplicaSetsTable.Fields.ReplicaSetId,
    [] (TNodeSegment* segment) { return &segment->MultiClusterReplicaSets(); },
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().NodeSegment(); },
};

TNodeSegment::TNodeSegment(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
    , PodSets_(this, &PodSetsSchema)
    , ReplicaSets_(this, &ReplicaSetsSchema)
    , MultiClusterReplicaSets_(this, &MultiClusterReplicaSetsSchema)
{ }

EObjectType TNodeSegment::GetType() const
{
    return EObjectType::NodeSegment;
}

bool TNodeSegment::IsBuiltin() const
{
    return GetId() == DefaultNodeSegmentId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

