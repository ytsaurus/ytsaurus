#include "node_type_handler.h"
#include "type_handler_detail.h"
#include "node.h"
#include "pod.h"
#include "db_schema.h"

#include <yp/server/net/helpers.h>

#include <yt/core/net/address.h>

namespace NYP {
namespace NServer {
namespace NObjects {

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
    {
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

                MakeFallbackAttributeSchema()
                    ->SetUpdatable()
                    ->SetAttribute(TNode::TStatus::OtherSchema)
            });

        ControlAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("update_hfsm_state")
                    ->SetControl<TNode, NClient::NApi::NProto::TNodeControl_TUpdateHfsmState>(std::bind(&TNodeTypeHandler::UpdateHfsmState, _1, _2, _3))
            });
    }

    virtual const TDbTable* GetTable() override
    {
        return &NodesTable;
    }

    virtual const TDbField* GetIdField() override
    {
        return &NodesTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TNode(id, this, session));
    }

    virtual void BeforeObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* node = object->As<TNode>();
        node->UpdateHfsmStatus(EHfsmState::Initial, "Node created");
        node->UpdateMaintenanceStatus(ENodeMaintenanceState::None, "Node created");
    }

    virtual void BeforeObjectRemoved(
        const TTransactionPtr& transaction,
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
    static void OnSpecUpdated(const TTransactionPtr& transaction, TNode* node)
    {
        transaction->ScheduleValidateNodeResources(node);
    }

    static void InitializeSpec(const TTransactionPtr& /*transaction*/, TNode* node, NClient::NApi::NProto::TNodeSpec* spec)
    {
        if (!spec->has_short_name()) {
            spec->set_short_name(BuildDefaultShortNodeName(node->GetId()));
        }
        if (!spec->hfsm().has_enable_sync()) {
            spec->mutable_hfsm()->set_enable_sync(true);
        }
        if (!spec->has_cpu_to_vcpu_factor()) {
            spec->set_cpu_to_vcpu_factor(1.0);
        }
    }

    static void ValidateSpec(const NClient::NApi::NProto::TNodeSpec& spec)
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
        const TTransactionPtr& /*transaction*/,
        TNode* node,
        const NClient::NApi::NProto::TNodeControl_TUpdateHfsmState& control)
    {
        auto state = static_cast<EHfsmState>(control.state());
        auto message = control.message();
        if (!message) {
            message = "State updated by client";
        }

        LOG_DEBUG("Updating node HFSM state (NodeId: %v, State: %v, Message: %v)",
            node->GetId(),
            state,
            message);

        node->UpdateHfsmStatus(state, message);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateNodeTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TNodeTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

