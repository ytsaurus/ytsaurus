#include "account.h"
#include "db_schema.h"
#include "deploy_ticket.h"
#include "stage.h"
#include "stage_type_handler.h"
#include "type_handler_detail.h"
#include "network_project.h"
#include "pod_type_handler.h"
#include "release_rule.h"
#include "config.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

#include <yp/client/api/proto/stage.pb.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TStageTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    TStageTypeHandler(NMaster::TBootstrap* bootstrap, TStageTypeHandlerConfigPtr config, TPodSpecValidationConfigPtr validationConfig)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Stage)
        , Config_(std::move(config))
        , PodSpecValidationConfig_(std::move(validationConfig))
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                //TODO(DEPLOY-1502): ParentAttribute after DEPLOY-1486 and DEPLOY-1501
                MakeAttributeSchema("project_id")
                    ->SetAttribute(TStage::ProjectIdSchema)
                    ->SetUpdatable()
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("account_id")
                    ->SetAttribute(TStage::TSpec::AccountSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetValidator<TStage>(std::bind(&TStageTypeHandler::ValidateAccount, this, _1, _2)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TStage::TSpec::EtcSchema)
                    ->SetUpdatable()
                    ->SetValidator<TStage>(std::bind(&TStageTypeHandler::ValidateSpecEtc, this, _1, _2))
            })
            ->SetExtensible()
            ->EnableHistory();

        StatusAttributeSchema_
            ->SetAttribute(TStage::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();

        if (Config_->EnableStatusHistory) {
            StatusAttributeSchema_
                ->EnableHistory(THistoryEnabledAttributeSchema()
                    .SetValueFilter<TStage>(std::bind(&TStageTypeHandler::DeployUnitConditionsHistoryFilter, this, _1)));
        }

        IdAttributeSchema_
            ->SetValidator<TStage>(ValidateId);
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TStage>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &StagesTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &StagesTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TStage>(id, this, session);
    }

private:
    const TStageTypeHandlerConfigPtr Config_;
    const TPodSpecValidationConfigPtr PodSpecValidationConfig_;

    void ValidateAccount(TTransaction* /*transaction*/, TStage* stage)
    {
        auto* account = stage->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }

    static void ThrowValidationError(const std::exception& ex, NClient::NApi::EErrorCode errorCode, const TString& id)
    {
        THROW_ERROR_EXCEPTION(errorCode,
            "Stage %Qv validation failed",
            id)
            << TErrorAttribute("object_id", id)
            << TErrorAttribute("object_type", EObjectType::Stage)
            << ex;
    }

    void ValidateSpecEtc(TTransaction* transaction, TStage* stage)
    {
        try {
            static const auto extractPodTemplateSpec = [] (const NClient::NApi::NProto::TDeployUnitSpec& deployUnit) -> const NClient::NApi::NProto::TPodTemplateSpec& {
                return deployUnit.has_replica_set()
                    ? deployUnit.replica_set().replica_set_template().pod_template_spec()
                    : deployUnit.multi_cluster_replica_set().replica_set().pod_template_spec();
            };

            const auto& oldUnits = stage->Spec().Etc().LoadOld().deploy_units();

            for (const auto& idAndDeployUnit : stage->Spec().Etc().Load().deploy_units()) {
                ValidateStageAndDeployUnitId(idAndDeployUnit.first, "Deploy unit id");
                const auto& deployUnit = idAndDeployUnit.second;

                if (!deployUnit.has_replica_set() && !deployUnit.has_multi_cluster_replica_set()) {
                    THROW_ERROR_EXCEPTION("Empty pod deploy primitive in deploy unit %Qv, stage %Qv",
                        idAndDeployUnit.first,
                        stage->GetId());
                }
                const auto& podTemplateSpec = extractPodTemplateSpec(deployUnit);
                const auto oldUnitsIt = oldUnits.find(idAndDeployUnit.first);
                const auto& oldPodTemplateSpec = oldUnitsIt != oldUnits.end()
                    ? extractPodTemplateSpec(oldUnitsIt->second)
                    : NClient::NApi::NProto::TPodTemplateSpec::default_instance();
                ValidateDeployPodSpecTemplate(
                    Bootstrap_->GetAccessControlManager(),
                    transaction,
                    oldPodTemplateSpec.spec(),
                    podTemplateSpec.spec(),
                    PodSpecValidationConfig_);
                ValidatePodAgentSpec(podTemplateSpec.spec().pod_agent_payload().spec(), deployUnit.images_for_boxes());

                if (deployUnit.has_tvm_config()) {
                    ValidateTvmConfig(deployUnit.tvm_config());
                }

                if (deployUnit.has_network_defaults()) {
                    const auto& oldNetworkDefaults = oldUnitsIt != oldUnits.end()
                        ? oldUnitsIt->second.network_defaults()
                        : NClient::NApi::NProto::TNetworkDefaults::default_instance();
                    ValidateDefaultNetworkProject(transaction, deployUnit.network_defaults(), oldNetworkDefaults);
                }
            }
        } catch (const std::exception& ex) {
            ThrowValidationError(ex, NClient::NApi::EErrorCode::InvalidObjectSpec, stage->GetId());
        }
    }

    void ValidateDefaultNetworkProject(TTransaction* transaction, const NClient::NApi::NProto::TNetworkDefaults& networkDefaults,
        const NClient::NApi::NProto::TNetworkDefaults& oldNetworkDefaults)
    {
        if (!networkDefaults.network_id().empty() && networkDefaults.network_id() != oldNetworkDefaults.network_id()) {
            auto* networkProject = transaction->GetNetworkProject(networkDefaults.network_id());
            const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
            accessControlManager->ValidatePermission(networkProject, NAccessControl::EAccessControlPermission::Use);
        }
    }

    static void ValidateId(TTransaction* /*transaction*/, TStage* stage)
    {
        try {
            ValidateStageAndDeployUnitId(stage->GetId(), "Stage id");
        } catch (const std::exception& ex) {
            ThrowValidationError(ex, NClient::NApi::EErrorCode::InvalidObjectId, stage->GetId());
        }
    }

    bool DeployUnitConditionsHistoryFilter(TStage* stage)
    {
        const auto& oldUnitStatuses = stage->Status().LoadOld().deploy_units();
        const auto& newUnitStatuses = stage->Status().Load().deploy_units();
        if (oldUnitStatuses.size() != newUnitStatuses.size()) {
            return true;
        }
        for (const auto& [unitId, oldUnitStatus] : oldUnitStatuses) {
            const auto newUnitStatusesIt = newUnitStatuses.find(unitId);
            if (newUnitStatusesIt == newUnitStatuses.end()) {
                return true;
            }
            const auto& newUnitStatus = newUnitStatusesIt->second;
            if (oldUnitStatus.ready().status() != newUnitStatus.ready().status() ||
                oldUnitStatus.in_progress().status() != newUnitStatus.in_progress().status() ||
                oldUnitStatus.failed().status() != newUnitStatus.failed().status()) {
                return true;
            }
        }
        return false;
    }
};

std::unique_ptr<IObjectTypeHandler> CreateStageTypeHandler(NMaster::TBootstrap* bootstrap, TStageTypeHandlerConfigPtr config, TPodSpecValidationConfigPtr validationConfig)
{
    return std::unique_ptr<IObjectTypeHandler>(new TStageTypeHandler(bootstrap, std::move(config), std::move(validationConfig)));
}

void ValidateTvmConfig(const NClient::NApi::NProto::TTvmConfig& config)
{
    static auto getAliasOrAppId = [] (const NClient::NApi::NProto::TTvmApp& tvmApp) -> TString {
        return tvmApp.alias().empty() ? ToString(tvmApp.app_id()) : tvmApp.alias();
    };
    if (config.mode() != NClient::NApi::NProto::TTvmConfig_EMode_ENABLED) {
        return;
    }
    THashSet<TString> sourceAliases;
    for (const auto& client : config.clients()) {
        auto sourceAlias = getAliasOrAppId(client.source());
        if (!sourceAliases.insert(sourceAlias).second) {
            THROW_ERROR_EXCEPTION("Source alias %Qv is used multiple times in TVM config",
                sourceAlias);
        }

        THashSet<TString> destinationAliases;
        for (const auto& destination : client.destinations()) {
            auto destinationAlias = getAliasOrAppId(destination);
            if (!destinationAliases.insert(destinationAlias).second) {
                THROW_ERROR_EXCEPTION("Destination alias %Qv is used multiple times for source %Qv in TVM config",
                    destinationAlias,
                    client.source().app_id());
            }
        }
    }
}

void ValidateStageAndDeployUnitId(const TObjectId& id, const TString& description)
{
    static const re2::RE2 stageIdPattern("[A-Za-z0-9-_]+");
    static const size_t idLengthLimit = 40;
    if (!re2::RE2::FullMatch(id, stageIdPattern)) {
        THROW_ERROR_EXCEPTION("%v %Qv must match regexp %Qv",
            description,
            id,
            stageIdPattern.Pattern());
    }
    if (id.length() > idLengthLimit) {
        THROW_ERROR_EXCEPTION("%v %Qv length exceeds limit %lu",
            description,
            id,
            idLengthLimit);
    }
}

void ValidatePodAgentSpec(
    const NInfra::NPodAgent::API::TPodAgentSpec& spec,
    const google::protobuf::Map<google::protobuf::string, NClient::NApi::NProto::TDockerImageDescription>& imagesForBoxes)
{
    THashSet<TString> staticResourceIds;
    THashSet<TString> layerIds;
    THashSet<TString> volumeIds;
    THashSet<TString> boxIds;
    THashSet<TString> workloadIds;
    THashSet<TString> mutableWorkloadRefs;

    for (const auto& staticResource : spec.resources().static_resources()) {
        ValidatePodAgentObjectId(staticResource.id(), "Static resource id");
        if (!staticResourceIds.insert(staticResource.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate static resource %Qv",
                staticResource.id());
        }
    }

    for (const auto& layer : spec.resources().layers()) {
        ValidatePodAgentObjectId(layer.id(), "Layer id");
        if (!layerIds.insert(layer.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate layer %Qv",
                layer.id());
        }
    }

    for (const auto& volume : spec.volumes()) {
        ValidatePodAgentObjectId(volume.id(), "Volume id");
        if (!volumeIds.insert(volume.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate volume %Qv",
                volume.id());
        }

        for (const auto& layerId : volume.generic().layer_refs()) {
            if (!layerIds.contains(layerId)) {
                THROW_ERROR_EXCEPTION("Volume %Qv refers to non-existing layer %Qv",
                    volume.id(),
                    layerId);
            }
        }
    }

    for (const auto& box : spec.boxes()) {
        ValidatePodAgentObjectId(box.id(), "Box id");
        ValidatePodAgentObjectEnv(box.env(), box.id(), "box");
        if (!boxIds.insert(box.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate box %Qv",
                box.id());
        }

        for (const auto& mountedStaticResource : box.static_resources()) {
            if (!staticResourceIds.contains(mountedStaticResource.resource_ref())) {
                THROW_ERROR_EXCEPTION("Box %Qv refers to non-existing static resource %Qv",
                    box.id(),
                    mountedStaticResource.resource_ref());
            }
        }

        for (const auto& layerId : box.rootfs().layer_refs()) {
            if (!layerIds.contains(layerId)) {
                THROW_ERROR_EXCEPTION("Box %Qv refers to non-existing rootfs layer %Qv",
                    box.id(),
                    layerId);
            }
        }

        for (const auto& mountedVolume : box.volumes()) {
            if (!volumeIds.contains(mountedVolume.volume_ref())) {
                THROW_ERROR_EXCEPTION("Box %Qv refers to non-existing volume %Qv",
                    box.id(),
                    mountedVolume.volume_ref());
            }
        }
    }

    for (const auto& imageForBox: imagesForBoxes) {
        if (!boxIds.contains(imageForBox.first)) {
            THROW_ERROR_EXCEPTION("Unknown box %Qv specified for docker images", imageForBox.first);
        }
    }

    for (const auto& workload : spec.workloads()) {
        ValidatePodAgentObjectId(workload.id(), "Workload id");
        ValidatePodAgentObjectEnv(workload.env(), workload.id(), "workload");
        if (!workloadIds.insert(workload.id()).second) {
            THROW_ERROR_EXCEPTION("Duplicate workload %Qv",
                workload.id());
        }

        if (!boxIds.contains(workload.box_ref())) {
            THROW_ERROR_EXCEPTION("Workload %Qv refers to non-existing box %Qv",
                workload.id(),
                workload.box_ref());
        }
    }

    for (const auto& mutableWorkload : spec.mutable_workloads()) {
        if (!mutableWorkloadRefs.insert(mutableWorkload.workload_ref()).second) {
            THROW_ERROR_EXCEPTION("Multiple mutable workloads refer to the same workload %Qv",
                mutableWorkload.workload_ref());
        }

        if (!workloadIds.contains(mutableWorkload.workload_ref())) {
            THROW_ERROR_EXCEPTION("Mutable workload %Qv refers to non-existing workload %Qv",
                mutableWorkload.workload_ref());
        }
    }

    // Mutable workload missed for one or more workloads.
    if (workloadIds.size() != mutableWorkloadRefs.size()) {
        for (const auto& workload : spec.workloads()) {
            if (!mutableWorkloadRefs.contains(workload.id())) {
                THROW_ERROR_EXCEPTION("Mutable workload not found for workload %Qv",
                    workload.id());
            }
        }
    }
}

void ValidatePodAgentObjectId(const TString& id, const TString& description)
{
    static const re2::RE2 podAgentObjectIdPattern("[a-zA-Z0-9\\-\\_\\@\\:\\.]+");
    static const size_t idLengthLimit = 64;
    if (!re2::RE2::FullMatch(id, podAgentObjectIdPattern)) {
        THROW_ERROR_EXCEPTION("%v %Qv must match regexp %Qv",
            description,
            id,
            podAgentObjectIdPattern.Pattern());
    }
    if (id.length() > idLengthLimit) {
        THROW_ERROR_EXCEPTION("%v %Qv length exceeds limit %lu",
            description,
            id,
            idLengthLimit);
    }
}

void ValidatePodAgentObjectEnv(
    const google::protobuf::RepeatedPtrField<NInfra::NPodAgent::API::TEnvVar>& env,
    const TString& objectId,
    const TString& description)
{
    THashSet<TString> nameSet;
    for (const auto& envVariable : env) {
        if (!nameSet.insert(envVariable.name()).second) {
            THROW_ERROR_EXCEPTION("Multiple entries for environment variable %Qv in %v %Qv",
                envVariable.name(),
                description,
                objectId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

