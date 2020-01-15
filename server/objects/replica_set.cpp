#include "replica_set.h"
#include "account.h"
#include "node_segment.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TReplicaSet, TAccount> TReplicaSet::TSpec::AccountSchema{
    &ReplicaSetsTable.Fields.Spec_AccountId,
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().Account(); },
    [] (TAccount* account) { return &account->ReplicaSets(); }
};

const TManyToOneAttributeSchema<TReplicaSet, TNodeSegment> TReplicaSet::TSpec::NodeSegmentSchema{
    &ReplicaSetsTable.Fields.Spec_NodeSegmentId,
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().NodeSegment(); },
    [] (TNodeSegment* segment) { return &segment->ReplicaSets(); }
};

const TScalarAttributeSchema<TReplicaSet, TReplicaSet::TSpec::TEtc> TReplicaSet::TSpec::EtcSchema{
    &ReplicaSetsTable.Fields.Spec_Etc,
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().Etc(); }
};

TReplicaSet::TSpec::TSpec(TReplicaSet* replicaSet)
    : Account_(replicaSet, &AccountSchema)
    , NodeSegment_(replicaSet, &NodeSegmentSchema)
    , Etc_(replicaSet, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TReplicaSet, TReplicaSet::TStatus> TReplicaSet::StatusSchema{
    &ReplicaSetsTable.Fields.Status,
    [] (TReplicaSet* rs) { return &rs->Status(); }
};

TReplicaSet::TReplicaSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TReplicaSet::GetType() const
{
    return EObjectType::ReplicaSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

