#include "network_project.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TNetworkProject, ui32> TNetworkProject::TSpec::ProjectIdSchema{
    &NetworkProjectsTable.Fields.Spec_ProjectId,
    [] (TNetworkProject* project) { return &project->Spec().ProjectId(); }
};

TNetworkProject::TSpec::TSpec(TNetworkProject* project)
    : ProjectId_(project, &ProjectIdSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

TNetworkProject::TNetworkProject(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this)
{ }

EObjectType TNetworkProject::GetType() const
{
    return EObjectType::NetworkProject;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

