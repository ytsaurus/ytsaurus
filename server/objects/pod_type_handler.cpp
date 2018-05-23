#include "pod_type_handler.h"
#include "type_handler_detail.h"
#include "pod.h"
#include "node.h"
#include "pod_set.h"
#include "db_schema.h"

#include <yp/server/net/net_manager.h>

#include <yp/server/master/bootstrap.h>

#include <yp/server/nodes/porto.h>

#include <yp/server/scheduler/resource_manager.h>
#include <yp/server/scheduler/helpers.h>

#include <yp/client/api/proto/cluster_api.pb.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/protobuf_interop.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYP::NServer::NNodes;
using namespace NYP::NServer::NScheduler;

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

////////////////////////////////////////////////////////////////////////////////

class TPodTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TPodTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Pod)
    {
        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("pod_set_id")
                    ->SetParentAttribute()
                    ->SetMandatory()
            });

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("agent")
                    ->AddChildren({
                        MakeAttributeSchema("state")
                            ->SetAttribute(TPod::TStatus::TAgent::StateSchema),

                        MakeAttributeSchema("iss_payload")
                            ->SetAttribute(TPod::TStatus::TAgent::IssPayloadSchema)
                            ->SetUpdatable(),

                        MakeAttributeSchema("iss")
                            ->SetProtobufEvaluator<TPod, NClient::NApi::NClusterApiProto::HostCurrentState>(TPod::TStatus::TAgent::IssPayloadSchema)
                    }),

                MakeAttributeSchema("generation_number")
                    ->SetAttribute(TPod::TStatus::GenerationNumberSchema),

                MakeAttributeSchema("master_spec_timestamp")
                    ->SetPreevaluator<TPod>(std::bind(&TPodTypeHandler::PreevaluateMasterSpecTimestamp, this, _1, _2))
                    ->SetEvaluator<TPod>(std::bind(&TPodTypeHandler::EvaluateMasterSpecTimestamp, this, _1, _2, _3)),

                MakeAttributeSchema("agent_spec_timestamp")
                    ->SetAttribute(TPod::TStatus::AgentSpecTimestampSchema),

                MakeFallbackAttributeSchema()
                    ->SetAttribute(TPod::TStatus::OtherSchema)
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("iss_payload")
                    ->SetAttribute(TPod::TSpec::IssPayloadSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("iss")
                    ->SetProtobufEvaluator<TPod, NClient::NApi::NClusterApiProto::HostConfiguration>(TPod::TSpec::IssPayloadSchema)
                    ->SetProtobufSetter<TPod, NClient::NApi::NClusterApiProto::HostConfiguration>(TPod::TSpec::IssPayloadSchema),

                MakeAttributeSchema("node_id")
                    ->SetAttribute(TPod::TSpec::NodeSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("enable_scheduling")
                    ->SetAttribute(TPod::TSpec::EnableSchedulingSchema)
                    ->SetUpdatable(),

                MakeFallbackAttributeSchema()
                    ->SetAttribute(TPod::TSpec::OtherSchema)
                    ->SetUpdatable()
            })
            ->SetUpdateHandler<TPod>(std::bind(&TPodTypeHandler::OnSpecUpdated, this, _1, _2))
            ->SetValidator<TPod>(std::bind(&TPodTypeHandler::ValidateSpec, this, _1, _2));

        ControlAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("acknowledge_eviction")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TAcknowledgeEviction>(std::bind(&TPodTypeHandler::AcknowledgeEviction, _1, _2, _3))
            });
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::PodSet;
    }

    virtual const TDbField* GetIdField() override
    {
        return &PodSetsTable.Fields.Meta_Id;
    }

    virtual const TDbField* GetParentIdField() override
    {
        return &PodsTable.Fields.Meta_PodSetId;
    }

    virtual const TDbTable* GetTable() override
    {
        return &PodsTable;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TPodSet>()->Pods();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TPod>(id, parentId, this, session);
    }

    virtual void BeforeObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* pod = object->As<TPod>();

        const auto& netManager = Bootstrap_->GetNetManager();
        pod->Status().Other()->mutable_dns()->set_persistent_fqdn(netManager->BuildPersistentPodFqdn(pod));

        pod->UpdateEvictionStatus(EEvictionState::None, EEvictionReason::None, "Pod created");

        pod->Status().Agent().State() = EPodCurrentState::Unknown;
    }

    virtual void AfterObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* pod = object->As<TPod>();
        const auto* node = pod->Spec().Node().Load();

        if (pod->Spec().EnableScheduling().Load()) {
            if (node) {
                THROW_ERROR_EXCEPTION("Cannot enable scheduling for pod %Qv and force-assign it to node %Qv at the same time",
                    pod->GetId(),
                    node->GetId());
            }
            pod->UpdateSchedulingStatus(
                ESchedulingState::Pending,
                "Pod created and awaits scheduling");
        } else {
            if (node) {
                pod->UpdateSchedulingStatus(
                    ESchedulingState::Assigned,
                    Format("Pod created and force-assigned to node %Qv", node->GetId()));
            } else {
                pod->UpdateSchedulingStatus(
                    ESchedulingState::Disabled,
                    "Pod created with scheduling disabled");
            }
        }
    }

    virtual void AfterObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectRemoved(transaction, object);

        auto* pod = object->As<TPod>();

        const auto& netManager = Bootstrap_->GetNetManager();
        netManager->UpdatePodAddresses(transaction, pod);

        const auto& resourceManager = Bootstrap_->GetResourceManager();
        resourceManager->RevokePodFromNode(transaction, pod);
    }

private:
    void PreevaluateMasterSpecTimestamp(const TTransactionPtr& /*transaction*/, TPod* pod)
    {
        pod->Spec().UpdateTimestamp().ScheduleLoad();
    }

    void EvaluateMasterSpecTimestamp(const TTransactionPtr& /*transaction*/, TPod* pod, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .Value(pod->Spec().UpdateTimestamp().Load());
    }


    void OnSpecUpdated(const TTransactionPtr& transaction, TPod* pod)
    {
        transaction->ScheduleUpdatePodSpec(pod);
    }

    void ValidateSpec(const TTransactionPtr& transaction, TPod* pod)
    {
        if (pod->Spec().EnableScheduling().IsChanged() &&
            pod->Spec().EnableScheduling().Load() &&
            pod->Spec().Node().IsChanged() &&
            pod->Spec().Node().Load())
        {
            THROW_ERROR_EXCEPTION("Cannot re-enable scheduling for pod %Qv and force-assign it to node %Qv at the same time",
                pod->GetId(),
                pod->Spec().Node().Load()->GetId());
        }

        for (const auto& spec : pod->Spec().Other().Load().host_devices()) {
            ValidateHostDeviceSpec(spec);
        }

        for (const auto& spec : pod->Spec().Other().Load().sysctl_properties()) {
            ValidateSysctlProperty(spec);
        }

        ValidateDiskVolumeRequests(pod->Spec().Other().Load().disk_volume_requests());
        if (pod->Spec().Other().IsChanged() && pod->Spec().Node().Load()) {
            ValidateDiskVolumeRequestsUpdate(
                pod->Spec().Other().Load().disk_volume_requests(),
                pod->Spec().Other().LoadOld().disk_volume_requests());
        }
    }

    static void AcknowledgeEviction(
        const TTransactionPtr& /*transaction*/,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TAcknowledgeEviction& control)
    {
        if (pod->Status().Other().Load().eviction().state() != NClient::NApi::NProto::ES_REQUESTED) {
            THROW_ERROR_EXCEPTION("No eviction is currently requested for pod %Qv",
                pod->GetId());
        }

        auto message = control.message();
        if (!message) {
            message = "Eviction acknowledged by client";
        }

        LOG_DEBUG("Pod eviction acknowledged (PodId: %v, Message: %v)",
            pod->GetId(),
            message);

        pod->UpdateEvictionStatus(EEvictionState::Acknowledged, EEvictionReason::None, message);
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePodTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPodTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

