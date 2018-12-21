#include "replica_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TReplicaSet, TReplicaSet::TSpec> TReplicaSet::SpecSchema{
    &ReplicaSetsTable.Fields.Spec,
    [] (TReplicaSet* rs) { return &rs->Spec(); }
};

const TScalarAttributeSchema<TReplicaSet, TReplicaSet::TStatus> TReplicaSet::StatusSchema{
    &ReplicaSetsTable.Fields.Status,
    [] (TReplicaSet* rs) { return &rs->Status(); }
};

TReplicaSet::TReplicaSet(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this, &SpecSchema)
    , Status_(this, &StatusSchema)
{ }

EObjectType TReplicaSet::GetType() const
{
    return EObjectType::ReplicaSet;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

