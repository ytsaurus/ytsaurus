#include "release_rule.h"

#include "db_schema.h"
#include "deploy_ticket.h"
#include "stage.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TOneToManyAttributeSchema<TReleaseRule, TDeployTicket> TReleaseRule::DeployTicketsSchema{
    &ReleaseRuleToDeployTicketsTable,
    &ReleaseRuleToDeployTicketsTable.Fields.ReleaseRuleId,
    &ReleaseRuleToDeployTicketsTable.Fields.DeployTicketId,
    [] (TReleaseRule* releaseRule) { return &releaseRule->DeployTickets(); },
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().ReleaseRule(); },
};

const TScalarAttributeSchema<TReleaseRule, TReleaseRule::TSpec::TEtc> TReleaseRule::TSpec::EtcSchema{
    &ReleaseRulesTable.Fields.Spec_Etc,
    [] (TReleaseRule* releaseRule) { return &releaseRule->Spec().Etc(); }
};

TReleaseRule::TSpec::TSpec(TReleaseRule* releaseRule)
    : Etc_(releaseRule, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TReleaseRule, TReleaseRule::TStatus> TReleaseRule::StatusSchema{
    &ReleaseRulesTable.Fields.Status,
    [] (TReleaseRule* releaseRule) { return &releaseRule->Status(); }
};

TReleaseRule::TReleaseRule(
    const TObjectId& id,
    const TObjectId& stageId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, stageId, typeHandler, session)
    , Stage_(this)
    , DeployTickets_(this, &DeployTicketsSchema)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TReleaseRule::GetType() const
{
    return EObjectType::ReleaseRule;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

