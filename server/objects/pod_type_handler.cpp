#include "pod_type_handler.h"
#include "type_handler_detail.h"
#include "pod.h"
#include "node.h"
#include "pod_set.h"
#include "account.h"
#include "network_project.h"
#include "db_schema.h"

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

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYP::NServer::NNodes;
using namespace NYP::NServer::NScheduler;

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto DefaultVcpuLimit = 1000;
static constexpr auto DefaultVcpuGuarantee = 1000;
static constexpr auto DefaultMemoryLimit = 100_MB;
static constexpr auto DefaultMemoryGuarantee = 100_MB;

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
                            ->SetProtobufEvaluator<TPod, NClient::NApi::NClusterApiProto::HostCurrentState>(TPod::TStatus::TAgent::IssPayloadSchema),

                        MakeAttributeSchema("pod_agent_payload")
                            ->SetAttribute(TPod::TStatus::TAgent::PodAgentPayloadSchema)
                            ->SetUpdatable(),

                        MakeFallbackAttributeSchema()
                            ->SetAttribute(TPod::TStatus::TAgent::OtherSchema)
                    }),

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

        auto* resourceRequests = pod->Spec().Other()->mutable_resource_requests();
        resourceRequests->set_vcpu_limit(DefaultVcpuLimit);
        resourceRequests->set_vcpu_guarantee(DefaultVcpuGuarantee);
        resourceRequests->set_memory_limit(DefaultMemoryLimit);
        resourceRequests->set_memory_guarantee(DefaultMemoryGuarantee);

        const auto& netManager = Bootstrap_->GetNetManager();
        pod->Status().Other()->mutable_dns()->set_persistent_fqdn(netManager->BuildPersistentPodFqdn(pod));

        pod->UpdateEvictionStatus(EEvictionState::None, EEvictionReason::None, "Pod created");

        pod->Status().Agent().State() = EPodCurrentState::Unknown;
    }

    virtual void AfterObjectCreated(
        TTransaction* transaction,
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

        transaction->ScheduleValidateAccounting(pod);
    }

    virtual void AfterObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectRemoved(transaction, object);

        auto* pod = object->As<TPod>();

        TInternetAddressManager internetAddressManager;
        TResourceManagerContext resourceManagerContext{
            Bootstrap_->GetNetManager().Get(),
            &internetAddressManager,
        };

        const auto& resourceManager = Bootstrap_->GetResourceManager();
        resourceManager->RevokePodFromNode(transaction, &resourceManagerContext, pod);

        transaction->ScheduleValidateAccounting(pod);
    }

private:
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

        if (spec.Other().IsChanged() ||
            spec.Node().IsChanged() ||
            spec.EnableScheduling().IsChanged())
        {
            transaction->ScheduleUpdatePodSpec(pod);
        }

        if (spec.Other().IsChanged()) {
            transaction->ScheduleValidateAccounting(pod);
        }

        pod->Spec().UpdateTimestamp().Touch();
    }

    void ValidateSpec(TTransaction* transaction, TPod* pod)
    {
        const auto& spec = pod->Spec();
        const auto& specOther = pod->Spec().Other();

        if (spec.Node().IsChanged()) {
            const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
            accessControlManager->ValidateSuperuser(AsStringBuf("change /spec/node_id"));
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

        if (specOther.IsChanged()) {
            for (const auto& spec : specOther.Load().host_devices()) {
                ValidateHostDeviceSpec(spec);
            }

            for (const auto& spec : specOther.Load().sysctl_properties()) {
                ValidateSysctlProperty(spec);
            }

            ValidateDiskVolumeRequests(pod);

            ValidateNetworkRequests(transaction, pod);
        }
    }

    static void AcknowledgeEviction(
        TTransaction* /*transaction*/,
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

        YT_LOG_DEBUG("Pod eviction acknowledged (PodId: %v, Message: %v)",
            pod->GetId(),
            message);

        pod->UpdateEvictionStatus(EEvictionState::Acknowledged, EEvictionReason::None, message);
    }

    static void ValidateDiskVolumeRequests(TPod* pod)
    {
        THashSet<TString> ids;
        const auto& requests = pod->Spec().Other().Load().disk_volume_requests();
        for (const auto& request : requests) {
            if (!ids.insert(request.id()).second) {
                THROW_ERROR_EXCEPTION("Duplicate disk volume request %Qv",
                    request.id());
            }
            if (!request.has_quota_policy() &&
                !request.has_exclusive_policy())
            {
                THROW_ERROR_EXCEPTION("Missing policy in disk volume request %Qv",
                    request.id());
            }
        }
    }

    void ValidateNetworkRequests(TTransaction* transaction, TPod* pod)
    {
        auto validateNetworkProject = [&] (const TObjectId& networkProjectId) {
            auto* networkProject = transaction->GetNetworkProject(networkProjectId);
            const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
            accessControlManager->ValidatePermission(networkProject, EAccessControlPermission::Use);
        };

        for (const auto& request : pod->Spec().Other().Load().ip6_address_requests()) {
            validateNetworkProject(request.network_id());
        }

        for (const auto& request : pod->Spec().Other().Load().ip6_subnet_requests()) {
            if (request.has_network_id()) {
                validateNetworkProject(request.network_id());
            } else {
                const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
                accessControlManager->ValidateSuperuser("configure IP6 subnet request without network id");
            }
        }
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


    bool AreLabelsDynamicAttributesChanged(TPod* pod)
    {
        const auto& specDynamicAttributes = pod->Spec().DynamicAttributes().Load();

        auto oldLabels = pod->Labels().LoadOld();
        auto newLabels = pod->Labels().Load();
        for (const auto& key : specDynamicAttributes.labels()) {
            auto oldValue = oldLabels->FindChild(key);
            auto newValue = newLabels->FindChild(key);
            if (!AreNodesEqual(oldValue, newValue)) {
                return true;
            }
        }

        return false;
    }

    void BeforeLabelsUpdated(TTransaction* /*transaction*/, TPod* pod)
    {
        pod->Spec().DynamicAttributes().ScheduleLoad();
    }

    void OnLabelsUpdated(TTransaction* /*transaction*/, TPod* pod)
    {
        if (AreLabelsDynamicAttributesChanged(pod)) {
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
};

std::unique_ptr<IObjectTypeHandler> CreatePodTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPodTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

