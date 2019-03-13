#include "multi_cluster_replica_set.h"
#include "account.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TMultiClusterReplicaSet, TAccount> TMultiClusterReplicaSet::TSpec::AccountSchema{
    &MultiClusterReplicaSetsTable.Fields.Spec_AccountId,
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().Account(); },
    [] (TAccount* account) { return &account->MultiClusterReplicaSets(); }
};

const TScalarAttributeSchema<TMultiClusterReplicaSet, TMultiClusterReplicaSet::TSpec::TOther> TMultiClusterReplicaSet::TSpec::OtherSchema{
    &MultiClusterReplicaSetsTable.Fields.Spec_Other,
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().Other(); }
};

TMultiClusterReplicaSet::TSpec::TSpec(TMultiClusterReplicaSet* MultiClusterReplicaSet)
    : Account_(MultiClusterReplicaSet, &AccountSchema)
    , Other_(MultiClusterReplicaSet, &OtherSchema)
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

