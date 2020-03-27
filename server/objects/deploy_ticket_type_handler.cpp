#include "deploy_ticket_type_handler.h"

#include "db_schema.h"
#include "deploy_ticket.h"
#include "release.h"
#include "release_rule.h"
#include "stage.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

#include <yp/client/api/proto/deploy_patch.pb.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

////////////////////////////////////////////////////////////////////////////////

class TDeployTicketTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TDeployTicketTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::DeployTicket)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("stage_id")
                    ->SetParentIdAttribute()
                    ->SetMandatory()
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("release_id")
                    ->SetAttribute(TDeployTicket::TSpec::ReleaseSchema
                        .SetNullable(false))
                    ->SetMandatory(),

                MakeAttributeSchema("release_rule_id")
                    ->SetAttribute(TDeployTicket::TSpec::ReleaseRuleSchema
                        .SetNullable(false))
                    ->SetMandatory(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TDeployTicket::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetValidator<TDeployTicket>(ValidatePatches)
            ->SetUpdatable();

        StatusAttributeSchema_
            ->SetAttribute(TDeployTicket::StatusSchema)
            ->SetUpdatable();

        IdAttributeSchema_
            ->SetValidator<TDeployTicket>(ValidateId);

        ControlAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("commit")
                    ->SetControl<TDeployTicket, NClient::NApi::NProto::TDeployTicketControl_TCommitAction>(std::bind(&TDeployTicketTypeHandler::CommitTicket, _1, _2, _3)),

                MakeAttributeSchema("skip")
                    ->SetControl<TDeployTicket, NClient::NApi::NProto::TDeployTicketControl_TSkipAction>(std::bind(&TDeployTicketTypeHandler::SkipTicket, _1, _2, _3)),
            });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TDeployTicket>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::Stage;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<TDeployTicket>()->Stage().Load();
    }

    virtual const TDBField* GetIdField() override
    {
        return &DeployTicketsTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &DeployTicketsTable.Fields.Meta_StageId;
    }

    virtual const TDBTable* GetTable() override
    {
        return &DeployTicketsTable;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TStage>()->DeployTickets();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::make_unique<TDeployTicket>(id, parentId, this, session);
    }

private:
    static void ValidateDeployTicketAndDeployPatchId(const TObjectId& id, const TString& description)
    {
        static const re2::RE2 idPattern("[A-Za-z0-9-_]+");
        static const size_t idLengthLimit = 70;
        if (!re2::RE2::FullMatch(id, idPattern)) {
            THROW_ERROR_EXCEPTION("%v %Qv must match regexp %Qv",
                description,
                id,
                idPattern.Pattern());
        }
        if (id.length() > idLengthLimit) {
            THROW_ERROR_EXCEPTION("%v %Qv length exceeds limit %lu",
                description,
                id,
                idLengthLimit);
        }
    }

    static void ValidateId(TTransaction* /*transaction*/, const TDeployTicket* deployTicket)
    {
        ValidateDeployTicketAndDeployPatchId(deployTicket->GetId(), "Deploy ticket id");
    }

    static void ValidatePatches(TTransaction* /*transaction*/, const TDeployTicket* deployTicket)
    {
        const auto& spec = deployTicket->Spec().Etc().Load();
        if (spec.patches_size() == 0) {
            THROW_ERROR_EXCEPTION("Deploy patches map cannot be empty");
        }
        for (const auto& idAndDeployPatch : spec.patches()) {
            ValidateDeployTicketAndDeployPatchId(idAndDeployPatch.first, "Deploy patch id");
        }
    }

    static EDeployPatchActionType GetTicketState(const TDeployTicket* deployTicket)
    {
        return CheckedEnumCast<EDeployPatchActionType>(deployTicket->Status().Load().action().type());
    }

    static EDeployPatchActionType GetPatchState(const TDeployTicket* deployTicket, const TObjectId& patchId)
    {
        const auto& patch = GetPatchOrThrow(deployTicket->Status().Load().patches(), patchId, deployTicket->GetId(), "status");
        return CheckedEnumCast<EDeployPatchActionType>(patch.action().type());
    }

    static std::vector<TObjectId> GetAlivePatches(const TDeployTicket* deployTicket)
    {
        std::vector<TObjectId> alivePatches;
        for (const auto& patchIdToSpec : deployTicket->Spec().Etc().Load().patches()) {
            auto state = GetPatchState(deployTicket, patchIdToSpec.first);
            if (state != EDeployPatchActionType::Commit && state != EDeployPatchActionType::Skip) {
                alivePatches.push_back(patchIdToSpec.first);
            }
        }

        return alivePatches;
    }

    static void ValidatePatchStates(const TDeployTicket* deployTicket, const std::vector<TObjectId>& patchIds)
    {
        for (const auto& patchId : patchIds) {
            auto state = GetPatchState(deployTicket, patchId);
            if (state == EDeployPatchActionType::Commit || state == EDeployPatchActionType::Skip) {
                THROW_ERROR_EXCEPTION("Patch %Qv has terminal state %Qv",
                    patchId,
                    state);
            }
        }
    }

    static void ValidateTicketState(const TDeployTicket* deployTicket)
    {
        auto state = GetTicketState(deployTicket);
        if (state == EDeployPatchActionType::Commit || state == EDeployPatchActionType::Skip) {
            THROW_ERROR_EXCEPTION("Ticket %Qv has terminal state %Qv",
                deployTicket->GetId(),
                state);
        }
    }

    template <typename TDeployPatchColumn>
    static const TDeployPatchColumn& GetPatchOrThrow(
        const google::protobuf::Map<google::protobuf::string, TDeployPatchColumn>& patches,
        const TObjectId& patchId,
        const TObjectId& deployTicketId,
        const TString& patchColumnDescription)
    {
        auto patchIt = patches.find(patchId);
        if (patchIt == patches.end()) {
            THROW_ERROR_EXCEPTION("Patch %Qv does not exist in %Qv of deploy ticket %Qv",
                patchId,
                patchColumnDescription,
                deployTicketId);
        }

        return patchIt->second;
    }

    static const NClient::NApi::NProto::TSandboxResource& GetReleaseResourceOrThrow(
        const THashMap<TString, NClient::NApi::NProto::TSandboxResource>& resourceTypeToResource,
        const TString& resourceType,
        const TObjectId& patchId,
        const TObjectId& releaseId)
    {
        auto releaseResourceIt = resourceTypeToResource.find(resourceType);
        if (releaseResourceIt == resourceTypeToResource.end()) {
            THROW_ERROR_EXCEPTION("Sandbox resource type %Qv of patch %Qv does not exist in release %Qv",
                resourceType,
                patchId,
                releaseId);
        }

        return releaseResourceIt->second;
    }

    template <typename TResourceType>
    static TResourceType* GetUpdatableStaticResourceOrThrow(
        const THashMap<TObjectId, TResourceType*>& resourceIdToResource,
        const TString& resourceRef,
        const TString& resourceDescription,
        const TObjectId& deployUnitId,
        const TObjectId& stageId)
    {
        auto staticResourceIt = resourceIdToResource.find(resourceRef);
        if (staticResourceIt == resourceIdToResource.end()) {
            THROW_ERROR_EXCEPTION("%v id %Qv does not exist in deploy unit %Qv, stage %Qv",
                resourceDescription,
                resourceRef,
                deployUnitId,
                stageId);
        }

        return staticResourceIt->second;
    }

    struct TStaticResourcesInfo
    {
        THashMap<TObjectId, NInfra::NPodAgent::API::TResource*> ResourceIdToResource;
        THashMap<TObjectId, NInfra::NPodAgent::API::TLayer*> LayerIdToLayer;
    };

    static const TStaticResourcesInfo& GetStaticResourcesInfoOrThrow(
        const THashMap<TObjectId, TStaticResourcesInfo>& deployUnitToStaticResources,
        const TObjectId& deployUnitId,
        const TObjectId& stageId)
    {
        auto staticResourcesInfoIt = deployUnitToStaticResources.find(deployUnitId);
        if (staticResourcesInfoIt == deployUnitToStaticResources.end()) {
            THROW_ERROR_EXCEPTION("Deploy unit %Qv does not exist in stage %Qv",
                deployUnitId,
                stageId);
        }

        return staticResourcesInfoIt->second;
    }

    static void UpdateResourceMeta(
        NInfra::NPodAgent::API::TSandboxResource* resourceMeta,
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease,
        const NClient::NApi::NProto::TSandboxResource& releaseResource)
    {
        resourceMeta->set_task_type(sandboxRelease.task_type());
        resourceMeta->set_task_id(sandboxRelease.task_id());
        resourceMeta->set_resource_type(releaseResource.type());
        resourceMeta->set_resource_id(releaseResource.resource_id());
    }

    static TString ConvertMD5ToChecksum(const TString& fileMD5)
    {
        return fileMD5.empty()
            ? "EMPTY:"
            : ("MD5:" + fileMD5);
    }

    static void UpdateStaticResource(
        NInfra::NPodAgent::API::TResource* resource,
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease,
        const NClient::NApi::NProto::TSandboxResource& releaseResource)
    {
        resource->set_url(releaseResource.skynet_id());
        resource->mutable_verification()->set_checksum(ConvertMD5ToChecksum(releaseResource.file_md5()));

        auto* resourceMeta = resource->mutable_meta()->mutable_sandbox_resource();
        UpdateResourceMeta(resourceMeta, sandboxRelease, releaseResource);
    }

    static void UpdateLayer(
        NInfra::NPodAgent::API::TLayer* layer,
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease,
        const NClient::NApi::NProto::TSandboxResource& releaseResource)
    {
        layer->set_url(releaseResource.skynet_id());
        layer->set_checksum(ConvertMD5ToChecksum(releaseResource.file_md5()));

        auto* resourceMeta = layer->mutable_meta()->mutable_sandbox_resource();
        UpdateResourceMeta(resourceMeta, sandboxRelease, releaseResource);
    }

    static void UpdateCommittedPatchStatus(
        TDeployTicket* deployTicket,
        const TObjectId& patchId,
        const TString& message,
        const TString& reason,
        TTimestamp startTimestamp)
    {
        YT_LOG_DEBUG("Deploy patch %v committed (Message: %v)",
            patchId,
            message);

        deployTicket->UpdatePatchStatus(
            patchId,
            EDeployPatchActionType::Commit,
            reason,
            message,
            startTimestamp);
    }

    static THashMap<TString, NClient::NApi::NProto::TSandboxResource> PrepareReleaseResources(
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease)
    {
        THashMap<TString, NClient::NApi::NProto::TSandboxResource> releaseResources;
        for (const auto& releaseResource : sandboxRelease.resources()) {
            if (!releaseResources.contains(releaseResource.type())) {
                releaseResources[releaseResource.type()] = releaseResource;
            }
        }

        return releaseResources;
    }

    static THashMap<TObjectId, TStaticResourcesInfo> PrepareStaticResources(TStage* stage)
    {
        THashMap<TObjectId, TStaticResourcesInfo> staticResources;

        for (auto& deployUnitIdToSpec : *stage->Spec().Etc()->mutable_deploy_units()) {

            const auto& deployUnitId = deployUnitIdToSpec.first;
            staticResources[deployUnitId] = {};

            auto& deployUnitSpec = deployUnitIdToSpec.second;

            auto* podTemplateSpec = deployUnitSpec.has_replica_set()
                ? deployUnitSpec.mutable_replica_set()->mutable_replica_set_template()->mutable_pod_template_spec()
                : deployUnitSpec.mutable_multi_cluster_replica_set()->mutable_replica_set()->mutable_pod_template_spec();

            auto* resources = podTemplateSpec->mutable_spec()->mutable_pod_agent_payload()->mutable_spec()->mutable_resources();

            for (auto& layer : *resources->mutable_layers()) {
                staticResources[deployUnitId].LayerIdToLayer[layer.id()] = &layer;
            }

            for (auto& staticResource : *resources->mutable_static_resources()) {
                staticResources[deployUnitId].ResourceIdToResource[staticResource.id()] = &staticResource;
            }
        }

        return staticResources;
    }

    static void ProcessStaticResources(
        TDeployTicket* deployTicket,
        TStage* stage,
        const std::vector<TObjectId>& patchIds,
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease,
        const TString& message,
        const TString& reason,
        TTimestamp startTimestamp)
    {
        auto resourceTypeToResource = PrepareReleaseResources(sandboxRelease);
        auto deployUnitToResources = PrepareStaticResources(stage);

        const auto& deployTicketId = deployTicket->GetId();
        const auto& stageId = stage->GetId();
        const auto& releaseId = deployTicket->Spec().Release().Load()->GetId();
        const auto& patches = deployTicket->Spec().Etc().Load().patches();

        for (const auto& patchId : patchIds) {
            const auto& patch = GetPatchOrThrow(patches, patchId, deployTicketId, "spec");

            const auto& staticResource = patch.sandbox().static_();

            const auto& resourceType = patch.sandbox().sandbox_resource_type();
            const auto& releaseResource = GetReleaseResourceOrThrow(resourceTypeToResource, resourceType, patchId, releaseId);

            const auto& deployUnitId = staticResource.deploy_unit_id();
            auto& staticResourcesInfo = GetStaticResourcesInfoOrThrow(deployUnitToResources, deployUnitId, stageId);

            if (staticResource.static_resource_ref()) {
                auto* updatableResource = GetUpdatableStaticResourceOrThrow(
                    staticResourcesInfo.ResourceIdToResource,
                    staticResource.static_resource_ref(),
                    "Static resource",
                    deployUnitId,
                    stageId);

                UpdateStaticResource(updatableResource, sandboxRelease, releaseResource);
                UpdateCommittedPatchStatus(deployTicket, patchId, message, reason, startTimestamp);
            } else if (staticResource.layer_ref()) {
                auto* updatableLayer = GetUpdatableStaticResourceOrThrow(
                    staticResourcesInfo.LayerIdToLayer,
                    staticResource.layer_ref(),
                    "Layer",
                    deployUnitId,
                    stageId);

                UpdateLayer(updatableLayer, sandboxRelease, releaseResource);
                UpdateCommittedPatchStatus(deployTicket, patchId, message, reason, startTimestamp);
            } else {
                THROW_ERROR_EXCEPTION("Empty static resource ref in deploy patch %Qv", patchId);
            }
        }
    }

    static NClient::NApi::NProto::TDockerImageDescription& GetDockerImageDescriptionOrThrow(
        google::protobuf::Map<google::protobuf::string, NClient::NApi::NProto::TDeployUnitSpec>& deployUnits,
        const TObjectId& deployUnitId,
        const TObjectId& boxId,
        const TObjectId& stageId)
    {
        auto deployUnitIt = deployUnits.find(deployUnitId);
        if (deployUnitIt == deployUnits.end()) {
            THROW_ERROR_EXCEPTION("Deploy unit %Qv does not exist in stage %Qv",
                deployUnitId,
                stageId);
        }

        auto& dockerImages = *deployUnitIt->second.mutable_images_for_boxes();

        auto dockerImageDescriptionIt = dockerImages.find(boxId);
        if (dockerImageDescriptionIt == dockerImages.end()) {
            THROW_ERROR_EXCEPTION("Docker image for box %Qv does not exist in deploy unit %Qv, stage %Qv",
                boxId,
                deployUnitId,
                stageId);
        }

        return dockerImageDescriptionIt->second;
    }

    static void ProcessDockerResources(
        TDeployTicket* deployTicket,
        TStage* stage,
        const std::vector<TObjectId>& patchIds,
        const TString& imageName,
        const TString& imageTag,
        const TString& message,
        const TString& reason,
        TTimestamp startTimestamp)
    {
        const auto& patches = deployTicket->Spec().Etc().Load().patches();
        auto& deployUnits = *stage->Spec().Etc()->mutable_deploy_units();

        for (const auto& patchId : patchIds) {
            const auto& patch = GetPatchOrThrow(patches, patchId, deployTicket->GetId(), "spec");
            const auto& dockerRef = patch.docker().docker_image_ref();

            auto& dockerImageDescription = GetDockerImageDescriptionOrThrow(
                deployUnits,
                dockerRef.deploy_unit_id(),
                dockerRef.box_id(),
                stage->GetId());

            dockerImageDescription.set_name(imageName);
            dockerImageDescription.set_tag(imageTag);

            UpdateCommittedPatchStatus(deployTicket, patchId, message, reason, startTimestamp);
        }
    }

    static void CommitPatches(
        TDeployTicket* deployTicket,
        const std::vector<TObjectId>& patchIds,
        const TString& message,
        const TString& reason,
        TTimestamp startTimestamp,
        bool isFullCommit)
    {
        ValidatePatchStates(deployTicket, patchIds);

        const auto* release = deployTicket->Spec().Release().Load();
        auto* stage = deployTicket->Stage().Load();

        auto patchesMessage = isFullCommit
            ? Format("DeployTicket was committed: %v", message)
            : message;

        if (release->Spec().Etc().Load().has_sandbox()) {
            const auto& sandboxRelease = release->Spec().Etc().Load().sandbox();
            ProcessStaticResources(
                deployTicket,
                stage,
                patchIds,
                sandboxRelease,
                patchesMessage,
                reason,
                startTimestamp);

        } else if (release->Spec().Etc().Load().has_docker()) {
            const auto& docker = release->Spec().Etc().Load().docker();
            ProcessDockerResources(
                deployTicket,
                stage,
                patchIds,
                docker.image_name(),
                docker.image_tag(),
                patchesMessage,
                reason,
                startTimestamp);

        } else {
            THROW_ERROR_EXCEPTION("Empty payload in release %Qv", release->GetId());
        }

        if (!patchIds.empty()) {
            stage->Spec().Etc()->set_revision(stage->Spec().Etc()->revision() + 1);
            stage->Spec().Etc()->mutable_revision_info()->set_description(message);
        }
    }

    static void CommitTicket(
        TTransaction* transaction,
        TDeployTicket* deployTicket,
        const NClient::NApi::NProto::TDeployTicketControl_TCommitAction& control)
    {
        DoTicketAction(
            deployTicket,
            control.options(),
            EDeployPatchActionType::Commit,
            CommitPatches,
            "commit",
            transaction->GetStartTimestamp());
    }

    static void SkipPatches(
        TDeployTicket* deployTicket,
        const std::vector<TObjectId>& patchIds,
        const TString& message,
        const TString& reason,
        TTimestamp startTimestamp,
        bool isFullSkip)
    {
        ValidatePatchStates(deployTicket, patchIds);

        auto patchesMessage = isFullSkip
            ? Format("DeployTicket was skipped: %v", message)
            : message;

        for (const auto& patchId : patchIds) {
            YT_LOG_DEBUG("Deploy patch %v skipped (Message: %v)",
                patchId,
                patchesMessage);

            deployTicket->UpdatePatchStatus(
                patchId,
                EDeployPatchActionType::Skip,
                reason,
                patchesMessage,
                startTimestamp);
        }
    }

    static void SkipTicket(
        TTransaction* transaction,
        TDeployTicket* deployTicket,
        const NClient::NApi::NProto::TDeployTicketControl_TSkipAction& control)
    {
        DoTicketAction(
            deployTicket,
            control.options(),
            EDeployPatchActionType::Skip,
            SkipPatches,
            "skip",
            transaction->GetStartTimestamp());
    }

    template <class TTicketAction>
    static void DoTicketAction(
        TDeployTicket* deployTicket,
        const NClient::NApi::NProto::TDeployTicketControl_TActionOptions& action,
        EDeployPatchActionType actionType,
        TTicketAction&& ticketAction,
        const TString& actionDescription,
        TTimestamp startTimestamp)
    {
        ValidateTicketState(deployTicket);

        if (action.patch_selector().type() == EDeployTicketPatchSelectorType::Full) {

            YT_LOG_DEBUG("Deploy ticket %v starting fully %v (TicketTitle: %v, Message: %v)",
                deployTicket->GetId(),
                actionDescription,
                deployTicket->Spec().Etc().Load().title(),
                action.message());

            ticketAction(
                deployTicket,
                GetAlivePatches(deployTicket),
                action.message(),
                action.reason(),
                startTimestamp,
                true);

            deployTicket->UpdateTicketStatus(
                actionType,
                action.reason(),
                action.message());

        } else if (action.patch_selector().type() == EDeployTicketPatchSelectorType::Partial){
            std::vector<TObjectId> patchIdsToAction;
            for (const auto& patchId : action.patch_selector().patch_ids()) {
                patchIdsToAction.push_back(patchId);
            }

            YT_LOG_DEBUG("Deploy ticket %v starting partially %v patches %v (TicketTitle: %v, Message: %v)",
                deployTicket->GetId(),
                actionDescription,
                patchIdsToAction,
                deployTicket->Spec().Etc().Load().title(),
                action.message());

            ticketAction(
                deployTicket,
                patchIdsToAction,
                action.message(),
                action.reason(),
                startTimestamp,
                false);

        } else {
            THROW_ERROR_EXCEPTION("Ticket %Qv has none action type", deployTicket->GetId());
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateDeployTicketTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TDeployTicketTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

