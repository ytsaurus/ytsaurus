#include "account.h"
#include "pod_set.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

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

const TScalarAttributeSchema<TAccount, TAccount::TSpec::TOther> TAccount::TSpec::OtherSchema{
    &AccountsTable.Fields.Spec_Other,
    [] (TAccount* account) { return &account->Spec().Other(); }
};

TAccount::TSpec::TSpec(TAccount* account)
    : Parent_(account, &ParentSchema)
    , Children_(account, &ChildrenSchema)
    , Other_(account, &OtherSchema)
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

TAccount::TAccount(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Status_(this, &StatusSchema)
    , Spec_(this)
    , PodSets_(this, &PodSetsSchema)
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

} // namespace NObjects
} // namespace NServer
} // namespace NYP

