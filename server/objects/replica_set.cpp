#include "replica_set.h"
#include "account.h"
#include "resource_cache.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TReplicaSet, TAccount> TReplicaSet::TSpec::AccountSchema{
    &ReplicaSetsTable.Fields.Spec_AccountId,
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().Account(); },
    [] (TAccount* account) { return &account->ReplicaSets(); }
};

const TScalarAttributeSchema<TReplicaSet, TReplicaSet::TSpec::TOther> TReplicaSet::TSpec::OtherSchema{
    &ReplicaSetsTable.Fields.Spec_Other,
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().Other(); }
};

TReplicaSet::TSpec::TSpec(TReplicaSet* replicaSet)
    : Account_(replicaSet, &AccountSchema)
    , Other_(replicaSet, &OtherSchema)
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
    , ResourceCache_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TReplicaSet::GetType() const
{
    return EObjectType::ReplicaSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

