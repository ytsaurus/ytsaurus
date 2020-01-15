#include "multi_cluster_replica_set.h"
#include "account.h"
#include "node_segment.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TMultiClusterReplicaSet, TAccount> TMultiClusterReplicaSet::TSpec::AccountSchema{
    &MultiClusterReplicaSetsTable.Fields.Spec_AccountId,
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().Account(); },
    [] (TAccount* account) { return &account->MultiClusterReplicaSets(); }
};

const TManyToOneAttributeSchema<TMultiClusterReplicaSet, TNodeSegment> TMultiClusterReplicaSet::TSpec::NodeSegmentSchema{
    &MultiClusterReplicaSetsTable.Fields.Spec_NodeSegmentId,
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().NodeSegment(); },
    [] (TNodeSegment* segment) { return &segment->MultiClusterReplicaSets(); }
};

const TScalarAttributeSchema<TMultiClusterReplicaSet, TMultiClusterReplicaSet::TSpec::TEtc> TMultiClusterReplicaSet::TSpec::EtcSchema{
    &MultiClusterReplicaSetsTable.Fields.Spec_Etc,
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().Etc(); }
};

TMultiClusterReplicaSet::TSpec::TSpec(TMultiClusterReplicaSet* MultiClusterReplicaSet)
    : Account_(MultiClusterReplicaSet, &AccountSchema)
    , NodeSegment_(MultiClusterReplicaSet, &NodeSegmentSchema)
    , Etc_(MultiClusterReplicaSet, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TMultiClusterReplicaSet, TMultiClusterReplicaSet::TStatus> TMultiClusterReplicaSet::StatusSchema{
    &MultiClusterReplicaSetsTable.Fields.Status,
    [] (TMultiClusterReplicaSet* MultiClusterReplicaSet) { return &MultiClusterReplicaSet->Status(); }
};

TMultiClusterReplicaSet::TMultiClusterReplicaSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TMultiClusterReplicaSet::GetType() const
{
    return EObjectType::MultiClusterReplicaSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

