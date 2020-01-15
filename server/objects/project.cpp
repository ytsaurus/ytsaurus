#include "project.h"

#include "account.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TProject, TAccount> TProject::TSpec::AccountSchema{
    &ProjectsTable.Fields.Spec_AccountId,
    [] (TProject* project) { return &project->Spec().Account(); },
    [] (TAccount* account) { return &account->Projects(); }
};

const TScalarAttributeSchema<TProject, TProject::TSpec::TEtc> TProject::TSpec::EtcSchema{
    &ProjectsTable.Fields.Spec_Etc,
    [] (TProject* project) { return &project->Spec().Etc(); }
};

TProject::TSpec::TSpec(TProject* project)
    : Account_(project, &AccountSchema)
    , Etc_(project, &EtcSchema)
{ }

const TScalarAttributeSchema<TProject, TString> TProject::OwnerIdSchema{
    &ProjectsTable.Fields.Meta_OwnerId,
    [] (TProject* project) { return &project->OwnerId(); }
};

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TProject, TProject::TStatus> TProject::StatusSchema{
    &ProjectsTable.Fields.Status,
    [] (TProject* project) { return &project->Status(); }
};

TProject::TProject(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , OwnerId_(this, &OwnerIdSchema)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TProject::GetType() const
{
    return EObjectType::Project;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
