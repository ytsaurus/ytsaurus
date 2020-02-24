#include "deploy_ticket.h"

#include "db_schema.h"
#include "release.h"
#include "release_rule.h"
#include "stage.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

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
    : Release_(deployTicket, &ReleaseSchema)
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
    const TObjectId& stageId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, stageId, typeHandler, session)
    , Stage_(this)
    , Spec_(this)
    , Status_(this, &StatusSchema)
{ }

EObjectType TDeployTicket::GetType() const
{
    return EObjectType::DeployTicket;
}

void TDeployTicket::UpdateTicketStatus(
    EDeployPatchActionType type,
    const TString& reason,
    const TString& message)
{
    auto* deployTicketAction = Status()->mutable_action();
    deployTicketAction->set_type(static_cast<NClient::NApi::NProto::EDeployPatchActionType>(type));
    deployTicketAction->set_reason(reason);
    deployTicketAction->set_message(message);
}

void TDeployTicket::UpdatePatchStatus(
    const TObjectId& patchId,
    EDeployPatchActionType type,
    const TString& reason,
    const TString& message,
    TTimestamp startTimestamp)
{
    (*Status()->mutable_patches())[patchId].set_stage_spec_timestamp(startTimestamp);

    auto* deployPatchAction = (*Status()->mutable_patches())[patchId].mutable_action();
    deployPatchAction->set_type(static_cast<NClient::NApi::NProto::EDeployPatchActionType>(type));
    deployPatchAction->set_reason(reason);
    deployPatchAction->set_message(message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

