#include "release.h"

#include "deploy_ticket.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TOneToManyAttributeSchema<TRelease, TDeployTicket> TRelease::DeployTicketsSchema{
    &ReleaseToDeployTicketsTable,
    &ReleaseToDeployTicketsTable.Fields.ReleaseId,
    &ReleaseToDeployTicketsTable.Fields.DeployTicketId,
    [] (TRelease* release) { return &release->DeployTickets(); },
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().Release(); },
};

const TScalarAttributeSchema<TRelease, TRelease::TSpec::TEtc> TRelease::TSpec::EtcSchema{
    &ReleasesTable.Fields.Spec_Etc,
    [] (TRelease* release) { return &release->Spec().Etc(); }
};

TRelease::TSpec::TSpec(TRelease* release)
    : Etc_(release, &EtcSchema)
{ }

const TScalarAttributeSchema<TRelease, TRelease::TStatus> TRelease::StatusSchema{
    &ReleasesTable.Fields.Status,
    [] (TRelease* release) { return &release->Status(); }
};

TRelease::TRelease(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , DeployTickets_(this, &DeployTicketsSchema)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TRelease::GetType() const
{
    return EObjectType::Release;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

