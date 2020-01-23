#include "node_type_handler.h"
#include "type_handler_detail.h"
#include "node.h"
#include "pod.h"
#include "db_schema.h"

#include <yp/server/net/helpers.h>

#include <yt/core/net/address.h>

namespace NYP::NServer::NObjects {

using namespace NServer::NNet;

using namespace NYT::NNet;

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

////////////////////////////////////////////////////////////////////////////////

class TNodeTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TNodeTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Node)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TNode::SpecSchema
                .SetInitializer(InitializeSpec)
                .SetValidator(ValidateSpec))
            ->SetUpdatable()
            ->SetUpdateHandler<TNode>(OnSpecUpdated);

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("agent_address")
                    ->SetAttribute(TNode::TStatus::AgentAddressSchema),

                MakeAttributeSchema("epoch_id")
                    ->SetAttribute(TNode::TStatus::EpochIdSchema),

                MakeAttributeSchema("last_seen_time")
                    ->SetAttribute(TNode::TStatus::LastSeenTimeSchema),

                MakeAttributeSchema("heartbeat_sequence_number")
                    ->SetAttribute(TNode::TStatus::HeartbeatSequenceNumberSchema),

                MakeAttributeSchema("host_manager")
                    ->SetAttribute(TNode::TStatus::HostManagerSchema),

                MakeEtcAttributeSchema()
                    ->SetUpdatable()
                    ->SetAttribute(TNode::TStatus::EtcSchema)
            });

        ControlAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("update_hfsm_state")
                    ->SetControl<TNode, NClient::NApi::NProto::TNodeControl_TUpdateHfsmState>(std::bind(&TNodeTypeHandler::UpdateHfsmState, _1, _2, _3)),

                MakeAttributeSchema("remove_alert")
                    ->SetControl<TNode, NClient::NApi::NProto::TNodeControl_TRemoveAlert>(std::bind(&TNodeTypeHandler::RemoveAlert, _1, _2, _3)),

                MakeAttributeSchema("add_alert")
                    ->SetControl<TNode, NClient::NApi::NProto::TNodeControl_TAddAlert>(std::bind(&TNodeTypeHandler::AddAlert, _1, _2, _3))
            });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TNode>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &NodesTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &NodesTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TNode(id, this, session));
    }

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* node = object->As<TNode>();
        node->UpdateHfsmStatus(
            EHfsmState::Initial,
            "Node created",
            /* maintenanceInfo */ std::nullopt);
        node->UpdateMaintenanceStatus(
            ENodeMaintenanceState::None,
            "Node created",
            /* infoUpdate */ TGenericClearUpdate());
    }

    virtual void BeforeObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* node = object->As<TNode>();
        const auto& pods = node->Pods().Load();
        if (!pods.empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove node %Qv since it has %v pod(s) assigned",
                node->GetId(),
                pods.size());
        }
    }

private:
    static void OnSpecUpdated(TTransaction* transaction, TNode* node)
    {
        transaction->ScheduleValidateNodeResources(node);
    }

    static void InitializeSpec(
        TTransaction* /*transaction*/,
        TNode* node,
        NClient::NApi::NProto::TNodeSpec* spec)
    {
        if (!spec->has_short_name()) {
            spec->set_short_name(BuildDefaultShortNodeName(node->GetId()));
        }
        if (!spec->hfsm().has_enable_sync()) {
            spec->mutable_hfsm()->set_enable_sync(true);
        }
    }

    static void ValidateSpec(
        TTransaction* /*transaction*/,
        TNode* /*node*/,
        const NClient::NApi::NProto::TNodeSpec& spec)
    {
        ValidateNodeShortName(spec.short_name());
        for (const auto& subnet : spec.ip6_subnets()) {
            ValidateMtnNetwork(TIP6Network::FromString(subnet.subnet()));
        }
        for (const auto& address : spec.ip6_addresses()) {
            TIP6Address::FromString(address.address());
        }
    }

    static void UpdateHfsmState(
        TTransaction* /*transaction*/,
        TNode* node,
        const NClient::NApi::NProto::TNodeControl_TUpdateHfsmState& control)
    {
        auto state = CheckedEnumCast<EHfsmState>(control.state());
        auto message = control.message();
        if (!message) {
            message = "State updated by the client";
        }
        std::optional<NClient::NApi::NProto::TMaintenanceInfo> maintenanceInfo;
        if (control.has_maintenance_info() || state == EHfsmState::PrepareMaintenance) {
            maintenanceInfo.emplace().CopyFrom(control.maintenance_info());
        }

        YT_LOG_DEBUG("Updating node HFSM state (NodeId: %v, State: %v, Message: %Qv, MaintenanceInfo: %v)",
            node->GetId(),
            state,
            message,
            maintenanceInfo);

        node->UpdateHfsmStatus(
            state,
            message,
            std::move(maintenanceInfo));
    }

    static void RemoveAlert(
        TTransaction* /*transaction*/,
        TNode* node,
        const NClient::NApi::NProto::TNodeControl_TRemoveAlert& control)
    {
        YT_LOG_DEBUG("Removing node alert (NodeId: %v, Uuid: %v, Message: %Qv)",
            node->GetId(),
            control.uuid(),
            control.message());
        node->RemoveAlert(control.uuid());
    }

    static void AddAlert(
        TTransaction* /*transaction*/,
        TNode* node,
        const NClient::NApi::NProto::TNodeControl_TAddAlert& control)
    {
        YT_LOG_DEBUG("Adding node alert (NodeId: %v, Type: %v, Description: %Qv)",
            node->GetId(),
            control.type(),
            control.description());
        node->AddAlert(control.type(), control.description());
    }
};

std::unique_ptr<IObjectTypeHandler> CreateNodeTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TNodeTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

