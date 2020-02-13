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

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("stage_id")
                    ->SetAttribute(TDeployTicket::TSpec::StageSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory()
                    ->SetValidator<TDeployTicket>(std::bind(&TDeployTicketTypeHandler::ValidateStage, this, _1, _2)),

                MakeAttributeSchema("release_id")
                    ->SetAttribute(TDeployTicket::TSpec::ReleaseSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory(),

                MakeAttributeSchema("release_rule_id")
                    ->SetAttribute(TDeployTicket::TSpec::ReleaseRuleSchema
                        .SetNullable(false))
                    ->SetUpdatable()
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

    virtual const TDBField* GetIdField() override
    {
        return &DeployTicketsTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &DeployTicketsTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TDeployTicket>(id, this, session);
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

    void ValidateStage(TTransaction* /*transaction*/, const TDeployTicket* deployTicket)
    {
        auto* stage = deployTicket->Spec().Stage().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(stage, EAccessControlPermission::Write);
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

    static EDeployPatchActionType GetPatchState(const TDeployTicket* deployTicket, const TString& patchId)
    {
        return CheckedEnumCast<EDeployPatchActionType>(deployTicket->Status().Load().patches().at(patchId).action().type());
    }

    static std::vector<TString> GetAlivePatches(const TDeployTicket* deployTicket)
    {
        std::vector<TString> alivePatches;
        for (const auto& patchIdToSpec : deployTicket->Spec().Etc().Load().patches()) {
            auto state = GetPatchState(deployTicket, patchIdToSpec.first);
            if (state != EDeployPatchActionType::Commit && state != EDeployPatchActionType::Skip) {
                alivePatches.push_back(patchIdToSpec.first);
            }
        }

        return alivePatches;
    }

    static void ValidatePatchesExistInDeployTicket(
        const TDeployTicket* deployTicket,
        const std::vector<TObjectId>& patchIds)
    {
        const auto& specPatches = deployTicket->Spec().Etc().Load().patches();
        const auto& statusPatches = deployTicket->Status().Load().patches();
        for (const auto& patchId : patchIds) {
            if (!specPatches.count(patchId)) {
                THROW_ERROR_EXCEPTION("Patch %Qv does not exist in spec of deploy ticket %Qv",
                    patchId,
                    deployTicket->GetId());
            }
            if (!statusPatches.count(patchId)) {
                THROW_ERROR_EXCEPTION("Patch %Qv does not exist in status of deploy ticket %Qv",
                    patchId,
                    deployTicket->GetId());
            }
        }
    }

    static void ValidatePatchStates(const TDeployTicket* deployTicket, const std::vector<TString>& patchIds)
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

    struct TStaticResourceInfo
    {
        TString PatchId;
        TString ResourceRef;
        TString DeployUnitId;
    };

    static void ValidateResourceInDeployUnit(const TStage* stage, const TStaticResourceInfo& staticResourceInfo)
    {
        if (!stage->Spec().Etc().Load().deploy_units().count(staticResourceInfo.DeployUnitId)) {
            THROW_ERROR_EXCEPTION("Deploy unit %Qv does not exist in stage %Qv",
                staticResourceInfo.DeployUnitId,
                stage->GetId());
        }

        const auto& deployUnit = stage->Spec().Etc().Load().deploy_units().at(staticResourceInfo.DeployUnitId);

        if (!deployUnit.has_replica_set() && !deployUnit.has_multi_cluster_replica_set()) {
            THROW_ERROR_EXCEPTION("Empty pod deploy primitive in deploy unit %Qv, stage %Qv",
                staticResourceInfo.DeployUnitId,
                stage->GetId());
        }

        const auto& podTemplateSpec = deployUnit.has_replica_set()
            ? deployUnit.replica_set().replica_set_template().pod_template_spec()
            : deployUnit.multi_cluster_replica_set().replica_set().pod_template_spec();

        bool resourceRefExistsInDeployUnit = false;
        for (const auto& staticResource : podTemplateSpec.spec().pod_agent_payload().spec().resources().static_resources()) {
            if (staticResource.id() == staticResourceInfo.ResourceRef) {
                resourceRefExistsInDeployUnit = true;
                break;
            }
        }

        if (!resourceRefExistsInDeployUnit) {
            THROW_ERROR_EXCEPTION("Static resource id %Qv does not exist in deploy unit %Qv, stage %Qv",
                staticResourceInfo.ResourceRef,
                staticResourceInfo.DeployUnitId,
                stage->GetId());
        }
    }

    static void ValidateReleaseResources(
        const TRelease* release,
        const TStage* stage,
        const THashMap<TString, std::vector<TStaticResourceInfo>>& staticResourceTypeToInfo)
    {
        THashMap<TString, std::vector<TStaticResourceInfo>> checkingResources = staticResourceTypeToInfo;
        for (const auto& releaseResource : release->Spec().Etc().Load().sandbox().resources()) {
            if (checkingResources.contains(releaseResource.type())) {

                auto& resourcesInfo = checkingResources[releaseResource.type()];
                while(!resourcesInfo.empty()) {
                    ValidateResourceInDeployUnit(stage, resourcesInfo.back());
                    resourcesInfo.pop_back();
                }

                checkingResources.erase(releaseResource.type());
            }
        }

        // All types from patch must be in release.
        if (!checkingResources.empty()) {
            TStringBuilder builder;
            TDelimitedStringBuilderWrapper delimitedBuilder(&builder);

            for (const auto& resource : checkingResources) {
                for (const auto& resourceInfo : resource.second) {
                    delimitedBuilder->AppendString(resourceInfo.PatchId);
                }
            }

            THROW_ERROR_EXCEPTION("Resource types of patches %Qv do not exist in release %Qv",
                builder.Flush(),
                release->GetId());
        }
    }

    static void UpdateResource(
        NInfra::NPodAgent::API::TResource& resource,
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease,
        const NClient::NApi::NProto::TSandboxResource& releaseResource)
    {
        resource.set_url(releaseResource.skynet_id());
        resource.mutable_verification()->set_checksum(releaseResource.file_md5().empty()
            ? "EMPTY:"
            : ("MD5:" + releaseResource.file_md5()));

        auto* sandboxResourceMeta = resource.mutable_meta()->mutable_sandbox_resource();
        sandboxResourceMeta->set_task_type(sandboxRelease.task_type());
        sandboxResourceMeta->set_task_id(sandboxRelease.task_id());
        sandboxResourceMeta->set_resource_type(releaseResource.type());
        sandboxResourceMeta->set_resource_id(releaseResource.resource_id());
    }

    static void ProcessStaticResources(
        TDeployTicket* deployTicket,
        TStage* stage,
        THashMap<TString, std::vector<TStaticResourceInfo>>& staticResourceTypeToInfo,
        const NClient::NApi::NProto::TSandboxRelease& sandboxRelease,
        const TString& message,
        const TString& reason)
    {
        for (const auto& releaseResource : sandboxRelease.resources()) {

            if (staticResourceTypeToInfo.contains(releaseResource.type())) {
                for (const auto& staticResourceInfo : staticResourceTypeToInfo.at(releaseResource.type())) {

                    auto& deployUnit = (*stage->Spec().Etc()->mutable_deploy_units())[staticResourceInfo.DeployUnitId];

                    auto* podTemplateSpec = deployUnit.has_replica_set()
                        ? deployUnit.mutable_replica_set()->mutable_replica_set_template()->mutable_pod_template_spec()
                        : deployUnit.mutable_multi_cluster_replica_set()->mutable_replica_set()->mutable_pod_template_spec();

                    for (auto& staticResource : *podTemplateSpec->mutable_spec()->mutable_pod_agent_payload()->mutable_spec()->mutable_resources()->mutable_static_resources()) {

                        if (staticResource.id() == staticResourceInfo.ResourceRef) {
                            UpdateResource(staticResource, sandboxRelease, releaseResource);

                            YT_LOG_DEBUG("Deploy patch %v committed (Message: %v)",
                                staticResourceInfo.PatchId,
                                message);

                            deployTicket->UpdatePatchStatus(
                                staticResourceInfo.PatchId,
                                EDeployPatchActionType::Commit,
                                reason,
                                message);
                        }
                    }
                }
                staticResourceTypeToInfo.erase(releaseResource.type());
            }
        }
    }

    static void CommitPatches(
        TDeployTicket* deployTicket,
        const std::vector<TString>& patchIds,
        const TString& message,
        const TString& reason)
    {
        ValidatePatchesExistInDeployTicket(deployTicket, patchIds);
        ValidatePatchStates(deployTicket, patchIds);

        THashMap<TString, std::vector<TStaticResourceInfo>> staticResourceTypeToInfo;
        const auto& patches = deployTicket->Spec().Etc().Load().patches();

        for (const auto& patchId : patchIds) {
            const auto& patch = patches.at(patchId);

            if (patch.has_sandbox()) {
                if (patch.sandbox().has_static_()) {
                    const auto& staticResource = patch.sandbox().static_();
                    if (staticResource.static_resource_ref()) {
                        staticResourceTypeToInfo[patch.sandbox().sandbox_resource_type()].push_back({
                            patchId,
                            staticResource.static_resource_ref(),
                            staticResource.deploy_unit_id()
                        });
                    } else if (staticResource.layer_ref()) {
                        // TODO(staroverovad): layer resources DEPLOY-1797
                        THROW_ERROR_EXCEPTION("Not implemented");
                    } else {
                        THROW_ERROR_EXCEPTION("Empty static resource ref in deploy patch %Qv", patchId);
                    }
                } else if (patch.sandbox().has_dynamic()) {
                    // TODO: dynamic resources after DEPLOY-1709
                    THROW_ERROR_EXCEPTION("Not implemented");
                } else {
                    THROW_ERROR_EXCEPTION("Empty sandbox resource ref in deploy patch %Qv", patchId);
                }
            } else if (patch.has_docker()) {
                // TODO(staroverovad): docker resources DEPLOY-1798
                THROW_ERROR_EXCEPTION("Not implemented");
            } else {
                THROW_ERROR_EXCEPTION("Empty payload in deploy patch %Qv", patchId);
            }
        }

        const auto* release = deployTicket->Spec().Release().Load();
        auto* stage = deployTicket->Spec().Stage().Load();

        if (release->Spec().Etc().Load().has_sandbox()) {
            ValidateReleaseResources(release, stage, staticResourceTypeToInfo);
            const auto& sandboxRelease = release->Spec().Etc().Load().sandbox();
            ProcessStaticResources(deployTicket, stage, staticResourceTypeToInfo, sandboxRelease, message, reason);

        } else if (release->Spec().Etc().Load().has_docker()) {
            // TODO(staroverovad): docker resources DEPLOY-1798
            THROW_ERROR_EXCEPTION("Not implemented");
        } else {
            THROW_ERROR_EXCEPTION("Empty payload in release %Qv", release->GetId());
        }

        if (!patchIds.empty()) {
            stage->Spec().Etc()->set_revision(stage->Spec().Etc()->revision() + 1);
        }
    }

    static void CommitTicket(
        TTransaction* /*transaction*/,
        TDeployTicket* deployTicket,
        const NClient::NApi::NProto::TDeployTicketControl_TCommitAction& control)
    {
        DoTicketAction(
            deployTicket,
            control.options(),
            EDeployPatchActionType::Commit,
            CommitPatches,
            "commit");
    }

    static void SkipPatches(
        TDeployTicket* deployTicket,
        const std::vector<TString>& patchIds,
        const TString& message,
        const TString& reason)
    {
        ValidatePatchesExistInDeployTicket(deployTicket, patchIds);
        ValidatePatchStates(deployTicket, patchIds);

        for (const auto& patchId : patchIds) {
            YT_LOG_DEBUG("Deploy patch %v skipped (Message: %v)",
                patchId,
                message);

            deployTicket->UpdatePatchStatus(
                patchId,
                EDeployPatchActionType::Skip,
                reason,
                message);
        }
    }

    static void SkipTicket(
        TTransaction* /*transaction*/,
        TDeployTicket* deployTicket,
        const NClient::NApi::NProto::TDeployTicketControl_TSkipAction& control)
    {
        DoTicketAction(
            deployTicket,
            control.options(),
            EDeployPatchActionType::Skip,
            SkipPatches,
            "skip");
    }

    template <class TTicketAction>
    static void DoTicketAction(
        TDeployTicket* deployTicket,
        const NClient::NApi::NProto::TDeployTicketControl_TActionOptions& action,
        EDeployPatchActionType actionType,
        TTicketAction&& ticketAction,
        const TString& actionDescription)
    {
        ValidateTicketState(deployTicket);

        if (action.patch_selector().type() == EDeployTicketPatchSelectorType::Full) {
            auto patchesMessage = Format("Parent %v: %v", action.reason(), action.message());

            YT_LOG_DEBUG("Deploy ticket %v starting fully %v (TicketTitle: %v, Message: %v)",
                deployTicket->GetId(),
                actionDescription,
                deployTicket->Spec().Etc().Load().title(),
                action.message());

            ticketAction(deployTicket, GetAlivePatches(deployTicket), patchesMessage, action.reason());

            deployTicket->UpdateTicketStatus(
                actionType,
                action.reason(),
                action.message());

        } else if (action.patch_selector().type() == EDeployTicketPatchSelectorType::Partial){
            std::vector<TString> patchIdsToAction;
            for (const auto& patchId : action.patch_selector().patch_ids()) {
                patchIdsToAction.push_back(patchId);
            }

            YT_LOG_DEBUG("Deploy ticket %v starting partially %v patches %v (TicketTitle: %v, Message: %v)",
                deployTicket->GetId(),
                actionDescription,
                patchIdsToAction,
                deployTicket->Spec().Etc().Load().title(),
                action.message());

            ticketAction(deployTicket, patchIdsToAction, action.message(), action.reason());
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

