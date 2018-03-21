#include "node.h"
#include "pod.h"
#include "resource.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

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

const TScalarAttributeSchema<TNode, TNode::TStatus::TOther> TNode::TStatus::OtherSchema{
    &NodesTable.Fields.Status_Other,
    [] (TNode* node) { return &node->Status().Other(); }
};

TNode::TStatus::TStatus(TNode* node)
    : AgentAddress_(node, &AgentAddressSchema)
    , EpochId_(node, &EpochIdSchema)
    , LastSeenTime_(node, &LastSeenTimeSchema)
    , HeartbeatSequenceNumber_(node, &HeartbeatSequenceNumberSchema)
    , Other_(node, &OtherSchema)
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

void TNode::UpdateHfsmStatus(EHfsmState state, const TString& message)
{
    auto* hfsm = Status().Other()->mutable_hfsm();
    hfsm->set_state(static_cast<NClient::NApi::NProto::EHfsmState>(state));
    hfsm->set_message(message);
    hfsm->set_last_updated(TInstant::Now().MicroSeconds());

    auto* maintenance = Status().Other()->mutable_maintenance();
    auto maintenanceState = static_cast<ENodeMaintenanceState>(maintenance->state());
    if (state == EHfsmState::PrepareMaintenance && maintenanceState != ENodeMaintenanceState::Requested) {
        UpdateMaintenanceStatus(
            ENodeMaintenanceState::Requested,
            Format("Maintenance requested by HFSM transition to %Qlv state",  state));
    }
    if (state == EHfsmState::Maintenance && maintenanceState != ENodeMaintenanceState::InProgress) {
        UpdateMaintenanceStatus(
            ENodeMaintenanceState::InProgress,
            Format("Maintenance is in progress due to HFSM transition to %Qlv state", state));
    }
    if (state != EHfsmState::PrepareMaintenance && state != EHfsmState::Maintenance && maintenanceState != ENodeMaintenanceState::None) {
        UpdateMaintenanceStatus(
            ENodeMaintenanceState::None,
            Format("Maintenance status reset due to HFSM transition to %Qlv state", state));
    }
}

void TNode::UpdateMaintenanceStatus(ENodeMaintenanceState state, const TString& message)
{
    auto* maintenance = Status().Other()->mutable_maintenance();
    maintenance->set_state(static_cast<NClient::NApi::NProto::ENodeMaintenanceState>(state));
    maintenance->set_message(message);
    maintenance->set_last_updated(TInstant::Now().MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

