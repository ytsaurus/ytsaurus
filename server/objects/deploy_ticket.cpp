#include "deploy_ticket.h"

#include "db_schema.h"
#include "release.h"
#include "release_rule.h"
#include "stage.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TDeployTicket, TStage> TDeployTicket::TSpec::StageSchema{
    &DeployTicketsTable.Fields.Spec_StageId,
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().Stage(); },
    [] (TStage* stage) { return &stage->DeployTickets(); }
};

const TManyToOneAttributeSchema<TDeployTicket, TRelease> TDeployTicket::TSpec::ReleaseSchema{
    &DeployTicketsTable.Fields.Spec_ReleaseId,
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().Release(); },
    [] (TRelease* release) { return &release->DeployTickets(); }
};

const TManyToOneAttributeSchema<TDeployTicket, TReleaseRule> TDeployTicket::TSpec::ReleaseRuleSchema{
    &DeployTicketsTable.Fields.Spec_ReleaseRuleId,
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().ReleaseRule(); },
    [] (TReleaseRule* releaseRule) { return &releaseRule->DeployTickets(); }
};

const TScalarAttributeSchema<TDeployTicket, TDeployTicket::TSpec::TEtc> TDeployTicket::TSpec::EtcSchema{
    &DeployTicketsTable.Fields.Spec_Etc,
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().Etc(); }
};

TDeployTicket::TSpec::TSpec(TDeployTicket* deployTicket)
    : Stage_(deployTicket, &StageSchema)
    , Release_(deployTicket, &ReleaseSchema)
    , ReleaseRule_(deployTicket, &ReleaseRuleSchema)
    , Etc_(deployTicket, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TDeployTicket, TDeployTicket::TStatus> TDeployTicket::StatusSchema{
    &DeployTicketsTable.Fields.Status,
    [] (TDeployTicket* deployTicket) { return &deployTicket->Status(); }
};

TDeployTicket::TDeployTicket(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TDeployTicket::GetType() const
{
    return EObjectType::DeployTicket;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

