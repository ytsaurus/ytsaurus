#include "stage.h"

#include "account.h"
#include "deploy_ticket.h"
#include "release_rule.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TStage, TAccount> TStage::TSpec::AccountSchema{
    &StagesTable.Fields.Spec_AccountId,
    [] (TStage* Stage) { return &Stage->Spec().Account(); },
    [] (TAccount* account) { return &account->Stages(); }
};

const TOneToManyAttributeSchema<TStage, TReleaseRule> TStage::ReleaseRulesSchema{
    &StageToReleaseRulesTable,
    &StageToReleaseRulesTable.Fields.StageId,
    &StageToReleaseRulesTable.Fields.ReleaseRuleId,
    [] (TStage* stage) { return &stage->ReleaseRules(); },
    [] (TReleaseRule* releaseRule) { return &releaseRule->Spec().Stage(); },
};

const TOneToManyAttributeSchema<TStage, TDeployTicket> TStage::DeployTicketsSchema{
    &StageToDeployTicketsTable,
    &StageToDeployTicketsTable.Fields.StageId,
    &StageToDeployTicketsTable.Fields.DeployTicketId,
    [] (TStage* stage) { return &stage->DeployTickets(); },
    [] (TDeployTicket* deployTicket) { return &deployTicket->Spec().Stage(); },
};

const TScalarAttributeSchema<TStage, TStage::TSpec::TEtc> TStage::TSpec::EtcSchema{
    &StagesTable.Fields.Spec_Etc,
    [] (TStage* Stage) { return &Stage->Spec().Etc(); }
};

TStage::TSpec::TSpec(TStage* stage)
    : Account_(stage, &AccountSchema)
    , Etc_(stage, &EtcSchema)
{ }

const TScalarAttributeSchema<TStage, TString> TStage::ProjectIdSchema{
    &StagesTable.Fields.Meta_ProjectId,
    [] (TStage* stage) { return &stage->ProjectId(); }
};

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TStage, TStage::TStatus> TStage::StatusSchema{
    &StagesTable.Fields.Status,
    [] (TStage* Stage) { return &Stage->Status(); }
};

TStage::TStage(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , ProjectId_(this, &ProjectIdSchema)
    , ReleaseRules_(this, &ReleaseRulesSchema)
    , DeployTickets_(this, &DeployTicketsSchema)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TStage::GetType() const
{
    return EObjectType::Stage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

