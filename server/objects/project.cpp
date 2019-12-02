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

TProject::TSpec::TSpec(TProject* project)
    : Account_(project, &AccountSchema)
{ }

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
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TProject::GetType() const
{
    return EObjectType::Project;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
