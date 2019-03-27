#include "account.h"
#include "pod.h"
#include "pod_set.h"
#include "replica_set.h"
#include "multi_cluster_replica_set.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TAccount, TAccount> TAccount::TSpec::ParentSchema{
    &AccountsTable.Fields.Spec_ParentId,
    [] (TAccount* account) { return &account->Spec().Parent(); },
    [] (TAccount* account) { return &account->Spec().Children(); }
};

const TOneToManyAttributeSchema<TAccount, TAccount> TAccount::TSpec::ChildrenSchema{
    &AccountParentToChildrenTable,
    &AccountParentToChildrenTable.Fields.ParentId,
    &AccountParentToChildrenTable.Fields.ChildId,
    [] (TAccount* account) { return &account->Spec().Children(); },
    [] (TAccount* account) { return &account->Spec().Parent(); },
};

const TScalarAttributeSchema<TAccount, TAccount::TSpec::TEtc> TAccount::TSpec::EtcSchema{
    &AccountsTable.Fields.Spec_Etc,
    [] (TAccount* account) { return &account->Spec().Etc(); }
};

TAccount::TSpec::TSpec(TAccount* account)
    : Parent_(account, &ParentSchema)
    , Children_(account, &ChildrenSchema)
    , Etc_(account, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TAccount, TAccount::TStatus> TAccount::StatusSchema{
    &AccountsTable.Fields.Status,
    [] (TAccount* account) { return &account->Status(); }
};

const TOneToManyAttributeSchema<TAccount, TPodSet> TAccount::PodSetsSchema{
    &AccountToPodSetsTable,
    &AccountToPodSetsTable.Fields.AccountId,
    &AccountToPodSetsTable.Fields.PodSetId,
    [] (TAccount* account) { return &account->PodSets(); },
    [] (TPodSet* podSet) { return &podSet->Spec().Account(); },
};

const TOneToManyAttributeSchema<TAccount, TPod> TAccount::PodsSchema{
    &AccountToPodsTable,
    &AccountToPodsTable.Fields.AccountId,
    &AccountToPodsTable.Fields.PodSetId,
    [] (TAccount* account) { return &account->Pods(); },
    [] (TPod* pod) { return &pod->Spec().Account(); },
};

const TOneToManyAttributeSchema<TAccount, TReplicaSet> TAccount::ReplicaSetsSchema{
    &AccountToReplicaSetsTable,
    &AccountToReplicaSetsTable.Fields.AccountId,
    &AccountToReplicaSetsTable.Fields.ReplicaSetId,
    [] (TAccount* account) { return &account->ReplicaSets(); },
    [] (TReplicaSet* replicaSet) { return &replicaSet->Spec().Account(); },
};

const TOneToManyAttributeSchema<TAccount, TMultiClusterReplicaSet> TAccount::MultiClusterReplicaSetsSchema{
    &AccountToMultiClusterReplicaSetsTable,
    &AccountToMultiClusterReplicaSetsTable.Fields.AccountId,
    &AccountToMultiClusterReplicaSetsTable.Fields.ReplicaSetId,
    [] (TAccount* account) { return &account->MultiClusterReplicaSets(); },
    [] (TMultiClusterReplicaSet* replicaSet) { return &replicaSet->Spec().Account(); },
};

TAccount::TAccount(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Status_(this, &StatusSchema)
    , Spec_(this)
    , PodSets_(this, &PodSetsSchema)
    , Pods_(this, &PodsSchema)
    , ReplicaSets_(this, &ReplicaSetsSchema)
    , MultiClusterReplicaSets_(this, &MultiClusterReplicaSetsSchema)
{ }

EObjectType TAccount::GetType() const
{
    return EObjectType::Account;
}

bool TAccount::IsBuiltin() const
{
    return GetId() == TmpAccountId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

