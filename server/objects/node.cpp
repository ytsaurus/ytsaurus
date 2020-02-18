#include "node.h"

#include "db_schema.h"
#include "helpers.h"
#include "pod.h"
#include "resource.h"

namespace NYP::NServer::NObjects {

using NClient::NApi::NProto::TMaintenanceInfo;

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TNode, TString> TNode::TStatus::AgentAddressSchema{
    &NodesTable.Fields.Status_AgentAddress,
    [] (TNode* node) { return &node->Status().AgentAddress(); }
};

const TScalarAttributeSchema<TNode, NNodes::TEpochId> TNode::TStatus::EpochIdSchema{
    &NodesTable.Fields.Status_EpochId,
    [] (TNode* node) { return &node->Status().EpochId(); }
};

const TScalarAttributeSchema<TNode, TInstant> TNode::TStatus::LastSeenTimeSchema{
    &NodesTable.Fields.Status_LastSeenTime,
    [] (TNode* node) { return &node->Status().LastSeenTime(); }
};

const TScalarAttributeSchema<TNode, ui64> TNode::TStatus::HeartbeatSequenceNumberSchema{
    &NodesTable.Fields.Status_HeartbeatSequenceNumber,
    [] (TNode* node) { return &node->Status().HeartbeatSequenceNumber(); }
};

const TScalarAttributeSchema<TNode, TNode::TStatus::THostManager> TNode::TStatus::HostManagerSchema{
    &NodesTable.Fields.Status_HostManager,
    [] (TNode* node) { return &node->Status().HostManager(); }
};

const TScalarAttributeSchema<TNode, TNode::TStatus::TEtc> TNode::TStatus::EtcSchema{
    &NodesTable.Fields.Status_Etc,
    [] (TNode* node) { return &node->Status().Etc(); }
};

TNode::TStatus::TStatus(TNode* node)
    : AgentAddress_(node, &AgentAddressSchema)
    , EpochId_(node, &EpochIdSchema)
    , LastSeenTime_(node, &LastSeenTimeSchema)
    , HeartbeatSequenceNumber_(node, &HeartbeatSequenceNumberSchema)
    , HostManager_(node, &HostManagerSchema)
    , Etc_(node, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TNode, TNode::TSpec> TNode::SpecSchema{
    &NodesTable.Fields.Spec,
    [] (TNode* node) { return &node->Spec(); }
};

const TOneToManyAttributeSchema<TNode, TPod> TNode::PodsSchema{
    &NodeToPodsTable,
    &NodeToPodsTable.Fields.NodeId,
    &NodeToPodsTable.Fields.PodId,
    [] (TNode* node) { return &node->Pods(); },
    [] (TPod* pod) { return &pod->Spec().Node(); },
};

TNode::TNode(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Resources_(this)
    , Status_(this)
    , Spec_(this, &SpecSchema)
    , Pods_(this, &PodsSchema)
{ }

EObjectType TNode::GetType() const
{
    return EObjectType::Node;
}

TResource* TNode::GetCpuResourceOrThrow()
{
    for (auto* resource : Resources().Load()) {
        resource->Kind().ScheduleLoad();
    }
    for (auto* resource : Resources().Load()) {
        if (resource->Kind().Load() == EResourceKind::Cpu) {
            return resource;
        }
    }
    THROW_ERROR_EXCEPTION("No %Qlv resource registered for node %Qv",
        EResourceKind::Cpu,
        GetId());
}

////////////////////////////////////////////////////////////////////////////////

void TNode::UpdateHfsmStatus(
    EHfsmState state,
    const TString& message,
    std::optional<TMaintenanceInfo> maintenanceInfo)
{
    if (state == EHfsmState::PrepareMaintenance) {
        if (!maintenanceInfo.has_value()) {
            THROW_ERROR_EXCEPTION("Maintenance info for HFSM state %Qv of node %Qv must be specified",
                state,
                GetId());
        }
    } else {
        if (maintenanceInfo.has_value()) {
            THROW_ERROR_EXCEPTION("Maintenance info for HFSM state %Qv of node %Qv cannot be specified",
                state,
                GetId());
        }
    }

    auto* hfsm = Status().Etc()->mutable_hfsm();
    hfsm->set_state(static_cast<NClient::NApi::NProto::EHfsmState>(state));
    hfsm->set_message(message);
    hfsm->set_last_updated(TInstant::Now().MicroSeconds());

    const auto& oldMaintenance = Status().Etc().Load().maintenance();
    auto oldMaintenanceState = static_cast<ENodeMaintenanceState>(oldMaintenance.state());
    if (state == EHfsmState::PrepareMaintenance) {
        YT_VERIFY(maintenanceInfo);
        if (oldMaintenanceState != ENodeMaintenanceState::Requested || maintenanceInfo->uuid() != oldMaintenance.info().uuid()) {
            UpdateMaintenanceStatus(
                ENodeMaintenanceState::Requested,
                Format("Maintenance requested by HFSM transition to %Qlv state", state),
                std::move(*maintenanceInfo));
        }
    } else if (state == EHfsmState::Maintenance) {
        YT_VERIFY(!maintenanceInfo);
        if (oldMaintenanceState != ENodeMaintenanceState::InProgress) {
            UpdateMaintenanceStatus(
                ENodeMaintenanceState::InProgress,
                Format("Maintenance is in progress due to HFSM transition to %Qlv state", state),
                TGenericPreserveUpdate());
        }
    } else {
        YT_VERIFY(!maintenanceInfo);
        if (oldMaintenanceState != ENodeMaintenanceState::None) {
            UpdateMaintenanceStatus(
                ENodeMaintenanceState::None,
                Format("Maintenance status reset due to HFSM transition to %Qlv state", state),
                TGenericClearUpdate());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNode::UpdateMaintenanceStatus(
    ENodeMaintenanceState state,
    const TString& message,
    TGenericUpdate<TMaintenanceInfo> infoUpdate)
{
    auto* maintenance = Status().Etc().Get()->mutable_maintenance();

    maintenance->set_state(static_cast<NClient::NApi::NProto::ENodeMaintenanceState>(state));
    maintenance->set_message(message);
    maintenance->set_last_updated(TInstant::Now().MicroSeconds());

    Visit(infoUpdate,
        [&] (TGenericPreserveUpdate /* preserve */) {
        },
        [&] (TGenericClearUpdate /* clear */) {
            maintenance->clear_info();
        },
        [&] (TMaintenanceInfo& info) {
            if (info.uuid()) {
                THROW_ERROR_EXCEPTION("Maintenance uuid cannot be specified");
            }
            info.set_uuid(GenerateUuid());
            info.set_id(GenerateId(info.id()));

            maintenance->mutable_info()->Swap(&info);
        });
}

void TNode::RemoveAlert(
    const TObjectId& uuid)
{
    auto* alerts = Status().Etc().Get()->mutable_alerts();
    RemoveObjectWithUuid(alerts, uuid);
}

void TNode::AddAlert(
    const TString& type,
    const TString& description)
{
    if (!type) {
        THROW_ERROR_EXCEPTION("Alert type must be specified");
    }
    auto* alert = Status().Etc().Get()->mutable_alerts()->Add();
    alert->set_type(type);
    alert->set_uuid(GenerateUuid());
    auto now = TInstant::Now();
    alert->mutable_creation_time()->set_seconds(now.Seconds());
    alert->mutable_creation_time()->set_nanos(now.NanoSecondsOfSecond());
    alert->set_description(description);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
