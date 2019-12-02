#include "account.h"
#include "pod.h"
#include "pod_set.h"
#include "replica_set.h"
#include "multi_cluster_replica_set.h"
#include "stage.h"
#include "project.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TAccount, TAccount> TAccount::TSpec::ParentSchema{
    &AccountsTable.Fields.Spec_ParentId,
    [] (TAccount* account) { return &account->Spec().Parent(); },
    [] (TAccount* account) { return &account->Children(); }
};

const TScalarAttributeSchema<TAccount, TAccount::TSpec::TEtc> TAccount::TSpec::EtcSchema{
    &AccountsTable.Fields.Spec_Etc,
    [] (TAccount* account) { return &account->Spec().Etc(); }
};

TAccount::TSpec::TSpec(TAccount* account)
    : Parent_(account, &ParentSchema)
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

const TOneToManyAttributeSchema<TAccount, TStage> TAccount::StagesSchema{
    &AccountToStagesTable,
    &AccountToStagesTable.Fields.AccountId,
    &AccountToStagesTable.Fields.StageId,
    [] (TAccount* account) { return &account->Stages(); },
    [] (TStage* stage) { return &stage->Spec().Account(); },
};

const TOneToManyAttributeSchema<TAccount, TAccount> TAccount::ChildrenSchema{
    &AccountParentToChildrenTable,
    &AccountParentToChildrenTable.Fields.ParentId,
    &AccountParentToChildrenTable.Fields.ChildId,
    [] (TAccount* account) { return &account->Children(); },
    [] (TAccount* account) { return &account->Spec().Parent(); },
};

const TOneToManyAttributeSchema<TAccount, TProject> TAccount::ProjectsSchema{
    &AccountToProjectsTable,
    &AccountToProjectsTable.Fields.AccountId,
    &AccountToProjectsTable.Fields.ProjectId,
    [] (TAccount* account) { return &account->Projects(); },
    [] (TProject* project) { return &project->Spec().Account(); },
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
    , Stages_(this, &StagesSchema)
    , Children_(this, &ChildrenSchema)
    , Projects_(this, &ProjectsSchema)
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

