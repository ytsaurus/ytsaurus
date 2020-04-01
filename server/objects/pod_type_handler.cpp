#include "pod_type_handler.h"

#include "account.h"
#include "config.h"
#include "db_schema.h"
#include "ip4_address_pool.h"
#include "network_project.h"
#include "node.h"
#include "node_segment.h"
#include "pod.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "persistent_volume.h"
#include "virtual_service.h"
#include "type_handler_detail.h"

#include <yp/server/net/internet_address_manager.h>
#include <yp/server/net/net_manager.h>

#include <yp/server/master/bootstrap.h>

#include <yp/server/nodes/porto.h>
#include <yp/server/nodes/helpers.h>

#include <yp/server/scheduler/resource_manager.h>
#include <yp/server/scheduler/helpers.h>

#include <yp/server/access_control/access_control_manager.h>

#include <yp/client/api/proto/cluster_api.pb.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/protobuf_interop.h>

#include <contrib/libs/protobuf/util/message_differencer.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYP::NServer::NNodes;
using namespace NYP::NServer::NScheduler;

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

using ::google::protobuf::RepeatedPtrField;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Common checks for pod spec used in Pod objects for TPodSpecEtc and objects which aggregate TPodSpec.
template <class TPodSpecEtc>
void ValidatePodSpecEtc(
    TAccessControlManagerPtr accessControlManager,
    TTransaction* transaction,
    const TPodSpecEtc& oldPodSpecEtc,
    const TPodSpecEtc& newPodSpecEtc,
    const TPodSpecValidationConfigPtr& config);

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPodTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    TPodTypeHandler(NMaster::TBootstrap* bootstrap, TPodTypeHandlerConfigPtr config)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Pod)
        , Config_(std::move(config))
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("pod_set_id")
                    ->SetParentIdAttribute()
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
                            ->SetProtobufSetter<TPod, NClient::NApi::NClusterApiProto::HostCurrentState>(TPod::TStatus::TAgent::IssPayloadSchema),

                        MakeAttributeSchema("pod_agent_payload")
                            ->SetAttribute(TPod::TStatus::TAgent::PodAgentPayloadSchema)
                            ->SetUpdatable(),

                        MakeEtcAttributeSchema()
                            ->SetAttribute(TPod::TStatus::TAgent::EtcSchema)
                    }),

                MakeAttributeSchema("scheduling")
                    ->AddChildren({
                        MakeAttributeSchema("node_id")
                            ->SetAttribute(TPod::TStatus::TScheduling::NodeIdSchema),
                        MakeEtcAttributeSchema()
                            ->SetAttribute(TPod::TStatus::TScheduling::EtcSchema)
                    })
                    ->EnableHistory(THistoryEnabledAttributeSchema()
                        .SetValueFilter<TPod>(std::bind(&TPodTypeHandler::StatusSchedulingHistoryFilter, this, _1))),

                MakeAttributeSchema("generation_number")
                    ->SetAttribute(TPod::TStatus::GenerationNumberSchema),

                MakeAttributeSchema("master_spec_timestamp")
                    ->SetPreevaluator<TPod>(std::bind(&TPodTypeHandler::PreevaluateMasterSpecTimestamp, this, _1, _2))
                    ->SetEvaluator<TPod>(std::bind(&TPodTypeHandler::EvaluateMasterSpecTimestamp, this, _1, _2, _3)),

                MakeAttributeSchema("agent_spec_timestamp")
                    ->SetAttribute(TPod::TStatus::AgentSpecTimestampSchema),

                MakeAttributeSchema("dynamic_resources")
                    ->SetAttribute(TPod::TStatus::DynamicResourcesSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("pod_dynamic_attributes")
                    ->SetOpaque()
                    ->SetPreevaluator<TPod>(std::bind(&TPodTypeHandler::PreevaluatePodDynamicAttributes, this, _1, _2))
                    ->SetEvaluator<TPod>(std::bind(&TPodTypeHandler::EvaluatePodDynamicAttributes, this, _1, _2, _3)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TPod::TStatus::EtcSchema)
                    ->EnableHistory(THistoryEnabledAttributeSchema()
                        .SetPath("/eviction")
                        .SetValueFilter<TPod>(std::bind(&TPodTypeHandler::StatusEvictionHistoryFilter, this, _1)))
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("iss_payload")
                    ->SetAttribute(TPod::TSpec::IssPayloadSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("iss")
                    ->SetProtobufEvaluator<TPod, NClient::NApi::NClusterApiProto::HostConfiguration>(TPod::TSpec::IssPayloadSchema)
                    ->SetProtobufSetter<TPod, NClient::NApi::NClusterApiProto::HostConfiguration>(TPod::TSpec::IssPayloadSchema),

                MakeAttributeSchema("pod_agent_payload")
                    ->SetAttribute(TPod::TSpec::PodAgentPayloadSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("node_id")
                    ->SetAttribute(TPod::TSpec::NodeSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("enable_scheduling")
                    ->SetAttribute(TPod::TSpec::EnableSchedulingSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("secrets")
                    ->SetAttribute(TPod::TSpec::SecretsSchema)
                    ->SetUpdatable()
                    ->SetReadPermission(NAccessControl::EAccessControlPermission::ReadSecrets),

                MakeAttributeSchema("account_id")
                    ->SetAttribute(TPod::TSpec::AccountSchema)
                    ->SetUpdatable()
                    ->SetUpdateHandler<TPod>(std::bind(&TPodTypeHandler::OnAccountUpdated, this, _1, _2))
                    ->SetValidator<TPod>(std::bind(&TPodTypeHandler::ValidateAccount, this, _1, _2)),

                MakeAttributeSchema("dynamic_resources")
                    ->SetAttribute(TPod::TSpec::DynamicResourcesSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("resource_cache")
                    ->SetAttribute(TPod::TSpec::ResourceCacheSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("dynamic_attributes")
                    ->SetAttribute(TPod::TSpec::DynamicAttributesSchema)
                    ->SetUpdatable(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TPod::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetUpdateHandler<TPod>(std::bind(&TPodTypeHandler::OnSpecUpdated, this, _1, _2))
            ->SetValidator<TPod>(std::bind(&TPodTypeHandler::ValidateSpec, this, _1, _2));

        ControlAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("acknowledge_eviction")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TAcknowledgeEviction>(std::bind(&TPodTypeHandler::AcknowledgeEviction, _1, _2, _3)),

                MakeAttributeSchema("request_eviction")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TRequestEviction>(std::bind(&TPodTypeHandler::RequestEviction, this, _1, _2, _3)),

                MakeAttributeSchema("abort_eviction")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TAbortEviction>(std::bind(&TPodTypeHandler::AbortEviction, this, _1, _2, _3)),

                MakeAttributeSchema("evict")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TEvict>(std::bind(&TPodTypeHandler::Evict, this, _1, _2, _3)),

                MakeAttributeSchema("touch_master_spec_timestamp")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TTouchMasterSpecTimestamp>(std::bind(&TPodTypeHandler::TouchMasterSpecTimestamp, this, _1, _2, _3)),

                MakeAttributeSchema("reallocate_resources")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TReallocateResources>(std::bind(&TPodTypeHandler::ReallocateResources, this, _1, _2, _3)),

                MakeAttributeSchema("acknowledge_maintenance")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TAcknowledgeMaintenance>(std::bind(&TPodTypeHandler::AcknowledgeMaintenance, this, _1, _2, _3)),

                MakeAttributeSchema("renounce_maintenance")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TRenounceMaintenance>(std::bind(&TPodTypeHandler::RenounceMaintenance, this, _1, _2, _3)),

                MakeAttributeSchema("add_scheduling_hint")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TAddSchedulingHint>(std::bind(&TPodTypeHandler::AddSchedulingHint, this, _1, _2, _3)),

                MakeAttributeSchema("remove_scheduling_hint")
                    ->SetControl<TPod, NClient::NApi::NProto::TPodControl_TRemoveSchedulingHint>(std::bind(&TPodTypeHandler::RemoveSchedulingHint, this, _1, _2, _3))
            });

        LabelsAttributeSchema_
            ->SetUpdatePrehandler<TPod>(std::bind(&TPodTypeHandler::BeforeLabelsUpdated, this, _1, _2))
            ->SetUpdateHandler<TPod>(std::bind(&TPodTypeHandler::OnLabelsUpdated, this, _1, _2));

        AnnotationsAttributeSchema_
            ->SetUpdateHandler<TPod>(std::bind(&TPodTypeHandler::BeforeAnnotationsUpdated, this, _1, _2))
            ->SetUpdateHandler<TPod>(std::bind(&TPodTypeHandler::OnAnnotationsUpdated, this, _1, _2));
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TPod>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::PodSet;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<TPod>()->PodSet().Load();
    }

    virtual const TDBField* GetIdField() override
    {
        return &PodSetsTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &PodsTable.Fields.Meta_PodSetId;
    }

    virtual const TDBTable* GetTable() override
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
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* pod = object->As<TPod>();

        const auto& netManager = Bootstrap_->GetNetManager();
        pod->Status().Etc()->mutable_dns()->set_persistent_fqdn(netManager->BuildPersistentPodFqdn(pod));

        pod->ResetAgentStatus();

        pod->UpdateEvictionStatus(EEvictionState::None, EEvictionReason::None, "Pod created");

        pod->UpdateMaintenanceStatus(
            EPodMaintenanceState::None,
            "Pod created",
            /* infoUpdate */ TGenericClearUpdate());
    }

    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* pod = object->As<TPod>();

        auto* resourceRequests = pod->Spec().Etc()->mutable_resource_requests();

        if (!resourceRequests->has_vcpu_limit() && !resourceRequests->has_vcpu_guarantee()) {
            resourceRequests->set_vcpu_limit(Config_->DefaultVcpuGuarantee);
            resourceRequests->set_vcpu_guarantee(Config_->DefaultVcpuGuarantee);
        } else if (!resourceRequests->has_vcpu_limit() && resourceRequests->has_vcpu_guarantee()) {
            resourceRequests->set_vcpu_limit(resourceRequests->vcpu_guarantee());
        }

        if (!resourceRequests->has_memory_limit() && !resourceRequests->has_memory_guarantee()) {
            resourceRequests->set_memory_limit(Config_->DefaultMemoryGuarantee);
            resourceRequests->set_memory_guarantee(Config_->DefaultMemoryGuarantee);
        } else if (!resourceRequests->has_memory_limit() && resourceRequests->has_memory_guarantee()) {
            resourceRequests->set_memory_limit(resourceRequests->memory_guarantee());
        }

        if (!resourceRequests->has_slot()) {
            resourceRequests->set_slot(Config_->DefaultSlot);
        }

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

        transaction->ScheduleValidateAccounting(pod);
    }

    virtual void AfterObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectRemoved(transaction, object);

        auto* pod = object->As<TPod>();

        NNet::TInternetAddressManager internetAddressManager;
        TResourceManagerContext resourceManagerContext{
            Bootstrap_->GetNetManager().Get(),
            &internetAddressManager,
        };

        const auto& resourceManager = Bootstrap_->GetResourceManager();
        resourceManager->RevokePodFromNode(transaction, &resourceManagerContext, pod);

        transaction->ScheduleValidateAccounting(pod);
    }

private:
    const TPodTypeHandlerConfigPtr Config_;

    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }

    void PreevaluateMasterSpecTimestamp(TTransaction* /*transaction*/, TPod* pod)
    {
        pod->Spec().UpdateTimestamp().ScheduleLoad();
    }

    void EvaluateMasterSpecTimestamp(TTransaction* /*transaction*/, TPod* pod, IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .Value(pod->Spec().UpdateTimestamp().Load());
    }

    void OnSpecUpdated(TTransaction* transaction, TPod* pod)
    {
        const auto& spec = pod->Spec();

        if (spec.Etc().IsChanged() ||
            spec.Node().IsChanged() ||
            spec.EnableScheduling().IsChanged())
        {
            transaction->ScheduleUpdatePodSpec(pod);
        }

        if (spec.Etc().IsChanged()) {
            transaction->ScheduleValidateAccounting(pod);
        }

        pod->Spec().UpdateTimestamp().Touch();

        auto* node = spec.Node().Load();
        if (node) {
            transaction->ScheduleNotifyAgent(node);
        }
    }

    void ValidateNodeSegmentConstraints(TPod* pod)
    {
        const auto& resourceRequests = pod->Spec().Etc().Load().resource_requests();
        const auto& oldResourceRequests = pod->Spec().Etc().LoadOld().resource_requests();

        if (oldResourceRequests.vcpu_limit() != resourceRequests.vcpu_limit() ||
            oldResourceRequests.vcpu_guarantee() != resourceRequests.vcpu_guarantee())
        {
            const auto* podSet = pod->PodSet().Load();

            podSet->Spec().Etc().ScheduleLoad();
            podSet->Spec().NodeSegment().ScheduleLoad();

            if (!podSet->Spec().Etc().Load().violate_node_segment_constraints().vcpu_guarantee_to_limit_ratio()) {
                const auto* nodeSegment = podSet->Spec().NodeSegment().Load();
                if (const auto& nodeSegmentSpec = nodeSegment->Spec().Load();
                    nodeSegmentSpec.pod_constraints().has_vcpu_guarantee_to_limit_ratio())
                {
                    const auto& constraint = nodeSegmentSpec.pod_constraints().vcpu_guarantee_to_limit_ratio();
                    ui64 vcpuLimitConstraint = constraint.multiplier() * resourceRequests.vcpu_guarantee() + constraint.additive();

                    if (resourceRequests.vcpu_limit() > vcpuLimitConstraint) {
                        THROW_ERROR_EXCEPTION("Violating node segment %Qv constraint of vcpu guarantee to limit ratio: "
                            "expected limit <= %v * guarantee + %v, got %v > %v",
                            nodeSegment->GetId(),
                            constraint.multiplier(),
                            constraint.additive(),
                            resourceRequests.vcpu_limit(),
                            vcpuLimitConstraint);
                    }
                }
            }
        }
    }

    void ValidatePodSetNodeSegmentPermission(
        TPod* pod,
        EAccessControlPermission permission,
        const NYPath::TYPath& attributePath)
    {
        auto* podSet = pod->PodSet().Load();
        auto* nodeSegment = podSet->Spec().NodeSegment().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(
            nodeSegment,
            permission,
            attributePath);
    }

    void ValidateThreadLimitChange(TPod* pod)
    {
        const auto& resourceRequests = pod->Spec().Etc().Load().resource_requests();
        const auto& oldResourceRequests = pod->Spec().Etc().LoadOld().resource_requests();
        if (oldResourceRequests.thread_limit() != resourceRequests.thread_limit()) {
            static const NYPath::TYPath attributePath("/access/scheduling/change_thread_limit");
            ValidatePodSetNodeSegmentPermission(
                pod,
                EAccessControlPermission::Use,
                attributePath);
        }
    }

    void ValidateSchedulingHints(TPod* pod)
    {
        const auto& schedulingHints = pod->Spec().Etc().Load().scheduling().hints();
        const auto& oldSchedulingHints = pod->Spec().Etc().LoadOld().scheduling().hints();

        bool equal = true;

        if (schedulingHints.size() != oldSchedulingHints.size()) {
            equal = false;
        } else {
            google::protobuf::util::MessageDifferencer messageDifferencer;
            for (int index = 0; index < schedulingHints.size(); ++index) {
                if (!messageDifferencer.Compare(schedulingHints.Get(index), oldSchedulingHints.Get(index))) {
                    equal = false;
                    break;
                }
            }
        }

        if (!equal) {
            THROW_ERROR_EXCEPTION("Changing scheduling hints manually is forbidden");
        }
    }

    void ValidateSpec(TTransaction* transaction, TPod* pod)
    {
        const auto& spec = pod->Spec();
        const auto& specEtc = pod->Spec().Etc();

        if (spec.Node().IsChanged()) {
            static const NYPath::TYPath attributePath("/access/scheduling/assign_pod_to_node");
            ValidatePodSetNodeSegmentPermission(
                pod,
                EAccessControlPermission::Use,
                attributePath);
        }

        if (spec.EnableScheduling().IsChanged() &&
            spec.EnableScheduling().Load() &&
            spec.Node().IsChanged() &&
            spec.Node().Load())
        {
            THROW_ERROR_EXCEPTION("Cannot re-enable scheduling for pod %Qv and force-assign it to node %Qv at the same time",
                pod->GetId(),
                spec.Node().Load()->GetId());
        }

        if (specEtc.IsChanged()) {
            ValidatePodSpecEtc(Bootstrap_->GetAccessControlManager(), transaction, specEtc.LoadOld(), specEtc.Load(), Config_->SpecValidation);
            ValidateNodeSegmentConstraints(pod);
            ValidateThreadLimitChange(pod);
            ValidateSchedulingHints(pod);
        }

        if (spec.IssPayload().IsChanged()) {
            // TODO(babenko): use read locks
            auto* podSet = pod->PodSet().Load();
            auto* nodeSegment = podSet->Spec().NodeSegment().Load();
            if (!nodeSegment->Spec().Load().enable_unsafe_porto()) {
                ValidateIssPodSpecSafe(pod);
            }
        }
    }

    static NClient::NApi::NProto::EEvictionState GetEvictionState(const TPod* pod)
    {
        return pod->Status().Etc().Load().eviction().state();
    }

    static void DoAcknowledgeEviction(
        TTransaction* /*transaction*/,
        TPod* pod,
        TString message)
    {
        if (GetEvictionState(pod) != NClient::NApi::NProto::ES_REQUESTED) {
            THROW_ERROR_EXCEPTION("No eviction is currently requested for pod %Qv",
                pod->GetId());
        }

        if (!message) {
            message = "Eviction acknowledged by client";
        }

        YT_LOG_DEBUG("Pod eviction acknowledged (PodId: %v, Message: %Qv)",
            pod->GetId(),
            message);

        pod->UpdateEvictionStatus(
            EEvictionState::Acknowledged,
            EEvictionReason::None,
            message);
    }

    static void AcknowledgeEviction(
        TTransaction* transaction,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TAcknowledgeEviction& control)
    {
        DoAcknowledgeEviction(
            transaction,
            pod,
            control.message());
    }

    void DoRequestEviction(
        TTransaction* /*transaction*/,
        TPod* pod,
        TString message,
        bool validateDisruptionBudget,
        EEvictionReason reason)
    {
        if (GetEvictionState(pod) != NClient::NApi::NProto::ES_NONE) {
            THROW_ERROR_EXCEPTION("Cannot request pod eviction for pod %Qv since current eviction state is not none",
                pod->GetId());
        }

        // NB! Concurrent pod assignment and eviction status changes are not possible
        // because any pod assignment change is guaranteed to overwrite eviction status.
        if (!pod->Spec().Node().Load()) {
            THROW_ERROR_EXCEPTION("Cannot request pod eviction because pod %Qv is not assigned to any node",
                pod->GetId());
        }

        if (!message) {
            message = "Eviction requested by client";
        }

        if (reason == EEvictionReason::None) {
            reason = EEvictionReason::Client;
        }

        YT_LOG_DEBUG("Pod eviction requested (PodId: %v, Message: %Qv, Reason: %v)",
            pod->GetId(),
            message,
            reason);

        pod->RequestEviction(
            reason,
            message,
            validateDisruptionBudget);
    }

    void RequestEviction(
        TTransaction* transaction,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TRequestEviction& control)
    {
        DoRequestEviction(
            transaction,
            pod,
            control.message(),
            control.validate_disruption_budget(),
            CheckedEnumCast<EEvictionReason>(control.reason()));
    }

    void AbortEviction(
        TTransaction* /*transaction*/,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TAbortEviction& control)
    {
        if (GetEvictionState(pod) != NClient::NApi::NProto::ES_REQUESTED) {
            THROW_ERROR_EXCEPTION("No eviction is currently requested for pod %Qv",
                pod->GetId());
        }

        auto message = control.message();
        if (!message) {
            message = "Eviction aborted by client";
        }

        YT_LOG_DEBUG("Pod eviction aborted (PodId: %v, Message: %Qv)",
            pod->GetId(),
            message);

        pod->UpdateEvictionStatus(EEvictionState::None, EEvictionReason::None, message);
    }

    void Evict(
        TTransaction* transaction,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TEvict& control)
    {
        DoRequestEviction(
            transaction,
            pod,
            control.message(),
            control.validate_disruption_budget(),
            EEvictionReason::Client);
        DoAcknowledgeEviction(
            transaction,
            pod,
            control.message());
    }

    void TouchMasterSpecTimestamp(
        TTransaction* /*transaction*/,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TTouchMasterSpecTimestamp& control)
    {
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidateSuperuser("touch master spec timestamp");

        auto message = control.message();
        if (!message) {
            message = "Pod master spec timestamp touched by client";
        }

        YT_LOG_DEBUG("Pod master spec timestamp touched (PodId: %v, Message: %Qv)",
            pod->GetId(),
            message);

        pod->Spec().UpdateTimestamp().Touch();
    }

    void ReallocateResources(
        TTransaction* transaction,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TReallocateResources& control)
    {
        // NB! We want to notify node about updates.
        pod->Spec().UpdateTimestamp().Touch();
        if (auto* node = pod->Spec().Node().Load(); node) {
            transaction->ScheduleNotifyAgent(node);
        }

        auto message = control.message();
        if (!message) {
            message = "Pod resources reallocation requested by client";
        }

        YT_LOG_DEBUG("Pod resources reallocation requested (PodId: %v, Message: %Qv)",
            pod->GetId(),
            message);

        transaction->ScheduleAllocateResources(pod);
    }

    void AcknowledgeMaintenance(
        TTransaction* /*transaction*/,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TAcknowledgeMaintenance& control)
    {
        if (pod->Status().Etc().Load().maintenance().state() != NClient::NApi::NProto::PMS_REQUESTED) {
            THROW_ERROR_EXCEPTION("No maintenance is currently requested for pod %Qv",
                pod->GetId());
        }

        auto message = control.message();
        if (!message) {
            message = "Maintenance acknowledged by client";
        }

        YT_LOG_DEBUG("Pod maintenance acknowledged (PodId: %v, Message: %Qv)",
            pod->GetId(),
            message);

        pod->UpdateMaintenanceStatus(
            EPodMaintenanceState::Acknowledged,
            message,
            /* infoUpdate */ TGenericPreserveUpdate());
    }

    void RenounceMaintenance(
        TTransaction* /*transaction*/,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TRenounceMaintenance& control)
    {
        const auto& podMaintenance = pod->Status().Etc().Load().maintenance();

        if (podMaintenance.state() != NClient::NApi::NProto::PMS_ACKNOWLEDGED) {
            THROW_ERROR_EXCEPTION("No maintenance is currently acknowledged for pod %Qv",
                pod->GetId());
        }

        {
            auto* node = pod->Spec().Node().Load();
            YT_VERIFY(node);
            const auto& nodeMaintenance = node->Status().Etc().Load().maintenance();
            if (nodeMaintenance.info().uuid() == podMaintenance.info().uuid()) {
                auto state = nodeMaintenance.state();
                if (state == NClient::NApi::NProto::NMS_ACKNOWLEDGED) {
                    node->UpdateMaintenanceStatus(
                        ENodeMaintenanceState::Requested,
                        Format("Node maintenance renounced due to pod %Qv maintenance renouncement", pod->GetId()),
                        /* infoUpdate */ TGenericPreserveUpdate());
                } else if (state == NClient::NApi::NProto::NMS_IN_PROGRESS) {
                    THROW_ERROR_EXCEPTION("In progress pod %Qv maintenance cannot be renounced",
                        pod->GetId());
                } else {
                    // XXX(bidzilya): Implicit store scheduling, use read lock instead.
                    node->Status().Etc().Get();
                }
            }
        }

        auto message = control.message();
        if (!message) {
            message = "Maintenance renounced by client";
        }

        YT_LOG_DEBUG("Pod maintenance renounced (PodId: %v, Message: %Qv)",
            pod->GetId(),
            message);

        pod->UpdateMaintenanceStatus(
            EPodMaintenanceState::Requested,
            message,
            /* infoUpdate */ TGenericPreserveUpdate());
    }

    void AddSchedulingHint(
        TTransaction* transaction,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TAddSchedulingHint& control)
    {
        if (!transaction->GetNode(control.node_id())->DoesExist()) {
            THROW_ERROR_EXCEPTION("Node with id %Qv does not exist",
                control.node_id());
        }

        YT_LOG_DEBUG("Add scheduling hint (PodId: %v, NodeId: %v, Strong: %v)",
            pod->GetId(),
            control.node_id(),
            control.strong());

        pod->AddSchedulingHint(control.node_id(), control.strong());
    }

    void RemoveSchedulingHint(
        TTransaction* /*transaction*/,
        TPod* pod,
        const NClient::NApi::NProto::TPodControl_TRemoveSchedulingHint& control)
    {
        YT_LOG_DEBUG("Remove scheduling hint (PodId: %v, Uuid: %v)",
            pod->GetId(),
            control.uuid());

        pod->RemoveSchedulingHint(control.uuid());
    }

    void ValidateAccount(TTransaction* /*transaction*/, TPod* pod)
    {
        auto* account = pod->Spec().Account().Load();
        if (!account) {
            return;
        }
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }

    void OnAccountUpdated(TTransaction* transaction, TPod* pod)
    {
        transaction->ScheduleValidateAccounting(pod);
    }


    void BeforeLabelsUpdated(TTransaction* /*transaction*/, TPod* pod)
    {
        pod->Spec().DynamicAttributes().ScheduleLoad();
    }

    void OnLabelsUpdated(TTransaction* /*transaction*/, TPod* pod)
    {
        if (pod->Labels().IsChanged()) {
            pod->Spec().UpdateTimestamp().Touch();
        }
    }


    bool AreAnnotationsDynamicAttributesChanged(TPod* pod)
    {
        const auto& specDynamicAttributes = pod->Spec().DynamicAttributes().Load();

        for (const auto& key : specDynamicAttributes.annotations()) {
            if (pod->Annotations().IsStoreScheduled(key)) {
                return true;
            }
        }

        return false;
    }

    void BeforeAnnotationsUpdated(TTransaction* /*transaction*/, TPod* pod)
    {
        pod->Spec().DynamicAttributes().ScheduleLoad();
    }

    void OnAnnotationsUpdated(TTransaction* /*transaction*/, TPod* pod)
    {
        if (AreAnnotationsDynamicAttributesChanged(pod)) {
            pod->Spec().UpdateTimestamp().Touch();
        }
    }


    void PreevaluatePodDynamicAttributes(TTransaction* /*transaction*/, TPod* pod)
    {
        SchedulePodDynamicAttributesLoad(pod);
    }

    void EvaluatePodDynamicAttributes(TTransaction* /*transaction*/, TPod* pod, IYsonConsumer* consumer)
    {
        auto attributes = BuildPodDynamicAttributes(pod);
        WriteProtobufMessage(consumer, attributes);
    }


    bool StatusSchedulingHistoryFilter(TPod* pod)
    {
        if (pod->Status().Scheduling().Etc().Load().state() != pod->Status().Scheduling().Etc().LoadOld().state()) {
            return true;
        }

        if (pod->Status().Scheduling().Etc().Load().has_error() != pod->Status().Scheduling().Etc().LoadOld().has_error()) {
            return true;
        }

        if (pod->Status().Scheduling().NodeId().Load() != pod->Status().Scheduling().NodeId().LoadOld()) {
            return true;
        }

        return false;
    }

    bool StatusEvictionHistoryFilter(TPod* pod)
    {
        const auto& oldEviction = pod->Status().Etc().LoadOld().eviction();
        const auto& newEviction = pod->Status().Etc().Load().eviction();

        if (oldEviction.state() != newEviction.state()) {
            return true;
        }

        if (oldEviction.reason() != newEviction.reason()) {
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateNetworkRequests(
    TAccessControlManagerPtr accessControlManager,
    TTransaction* transaction,
    const RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TIP6AddressRequest>& oldIp6AddressRequests,
    const RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TIP6AddressRequest>& newIp6AddressRequests,
    const RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TIP6SubnetRequest>& oldIp6SubnetRequests,
    const RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TIP6SubnetRequest>& newIp6SubnetRequests)
{
    auto validateUsePermission = [&] (TObject* object) {
        accessControlManager->ValidatePermission(object, NAccessControl::EAccessControlPermission::Use);
    };

    auto validateNetworkProject = [&] (const TObjectId& networkProjectId) {
        auto* networkProject = transaction->GetNetworkProject(networkProjectId);
        validateUsePermission(networkProject);
    };

    THashSet<TObjectId> oldVirtualServiceIds;
    THashSet<TObjectId> oldNetworkProjectIds;
    for (const auto& request : oldIp6AddressRequests) {
        oldNetworkProjectIds.insert(request.network_id());
        for (const auto& virtualServiceId : request.virtual_service_ids()) {
            oldVirtualServiceIds.insert(virtualServiceId);
        }
    }
    for (const auto& request : oldIp6SubnetRequests) {
        oldNetworkProjectIds.insert(request.network_id());
    }

    for (const auto& request : newIp6AddressRequests) {
        if (oldNetworkProjectIds.find(request.network_id()) == oldNetworkProjectIds.end()) {
            validateNetworkProject(request.network_id());
        }

        for (const auto& virtualServiceId : request.virtual_service_ids()) {
            if (oldVirtualServiceIds.find(virtualServiceId) == oldVirtualServiceIds.end()) {
                ValidateObjectId(EObjectType::VirtualService, virtualServiceId);
                if (!transaction->GetVirtualService(virtualServiceId)->DoesExist()) {
                    THROW_ERROR_EXCEPTION("Virtual service %Qv does not exist", virtualServiceId);
                }
            }
        }

        if (const auto& poolId = request.ip4_address_pool_id(); !poolId.empty()) {
            auto* pool = transaction->GetIP4AddressPool(poolId);
            validateUsePermission(pool);
        } else if (request.enable_internet()) {
            auto* pool = transaction->GetIP4AddressPool(DefaultIP4AddressPoolId);
            validateUsePermission(pool);
        }
    }

    for (const auto& request : newIp6SubnetRequests) {
        if (request.has_network_id()) {
            if (oldNetworkProjectIds.find(request.network_id()) == oldNetworkProjectIds.end()) {
                validateNetworkProject(request.network_id());
            }
        } else {
            accessControlManager->ValidateSuperuser("configure IP6 subnet request without network id");
        }
    }
}

template <class T>
void ValidateNoDuplicateResourceRequestIds(const T& items, TStringBuf resourceName)
{
    THashSet<TString> ids;
    for (const auto& item : items) {
        if (!ids.insert(item.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate %Qv request %Qv",
                resourceName,
                item.id());
        }
    }
}

void ValidateGpuRequests(const RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TGpuRequest>& requests)
{
    ValidateNoDuplicateResourceRequestIds(requests, "GPU");
}

void ValidateResourceRequests(
    const NClient::NApi::NProto::TPodSpec_TResourceRequests& resourceRequests,
    ui64 minVcpuGuarantee)
{
    if (resourceRequests.has_vcpu_guarantee() && resourceRequests.vcpu_guarantee() < minVcpuGuarantee) {
        THROW_ERROR_EXCEPTION("Invalid vcpu_gurantee value: expected >= %v, got %v",
            minVcpuGuarantee,
            resourceRequests.vcpu_guarantee());
    }
}

void ValidateDiskVolumeRequests(const RepeatedPtrField<NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>& requests)
{
    ValidateNoDuplicateResourceRequestIds(requests, "disk volume");
    for (const auto& request : requests) {
        if (!request.has_quota_policy() &&
            !request.has_exclusive_policy())
        {
            THROW_ERROR_EXCEPTION("Missing policy in disk volume request %Qv",
                request.id());
        }
    }
}

void ValidateCapabilities(const RepeatedPtrField<TString>& capabilities)
{
    for (const auto& capability : capabilities) {
        if (capability.Empty()) {
            THROW_ERROR_EXCEPTION("Capability name cannot be empty string");
        }
    }
}

template <class TPodSpecEtc>
void ValidatePodSpecEtc(
    TAccessControlManagerPtr accessControlManager,
    TTransaction* transaction,
    const TPodSpecEtc& podSpecEtcOld,
    const TPodSpecEtc& podSpecEtcNew,
    const TPodSpecValidationConfigPtr& config)
{
    for (const auto& spec : podSpecEtcNew.host_devices()) {
        ValidateHostDeviceSpec(spec);
    }

    for (const auto& spec : podSpecEtcNew.sysctl_properties()) {
        ValidateSysctlProperty(spec);
    }

    ValidateGpuRequests(podSpecEtcNew.gpu_requests());

    ValidateDiskVolumeRequests(podSpecEtcNew.disk_volume_requests());

    ValidateNetworkRequests(std::move(accessControlManager), transaction, podSpecEtcOld.ip6_address_requests(),
        podSpecEtcNew.ip6_address_requests(), podSpecEtcOld.ip6_subnet_requests(), podSpecEtcNew.ip6_subnet_requests());

    ValidateResourceRequests(podSpecEtcNew.resource_requests(), config->MinVcpuGuarantee);

    ValidateCapabilities(podSpecEtcNew.capabilities());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateDeployPodSpecTemplate(
    const TAccessControlManagerPtr& accessControlManager,
    TTransaction* transaction,
    const NClient::NApi::NProto::TPodSpec& oldPodSpec,
    const NClient::NApi::NProto::TPodSpec& newPodSpec,
    const TPodSpecValidationConfigPtr& config)
{
    ValidatePodSpecEtc(accessControlManager, transaction, oldPodSpec, newPodSpec, config);

    if (newPodSpec.has_iss_payload()) {
        THROW_ERROR_EXCEPTION("ISS payload is not supported in Deploy");
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreatePodTypeHandler(NMaster::TBootstrap* bootstrap, TPodTypeHandlerConfigPtr config)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPodTypeHandler(bootstrap, std::move(config)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
