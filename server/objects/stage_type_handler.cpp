#include "stage_type_handler.h"
#include "account.h"
#include "db_schema.h"
#include "stage.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

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
    explicit TStageTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Stage)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

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
                    ->SetValidator<TStage>(ValidateSpecEtc)
            })
            ->SetExtensible()
            ->SetHistoryEnabled();

        StatusAttributeSchema_
            ->SetAttribute(TStage::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();

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

    static void ValidateSpecEtc(TTransaction* /*transaction*/, TStage* stage)
    {
        try {
            for (const auto& idAndDeployUnit : stage->Spec().Etc().Load().deploy_units()) {
                ValidateStageAndDeployUnitId(idAndDeployUnit.first, "Deploy unit id");
                const auto& deployUnit = idAndDeployUnit.second;

                const auto& podTemplateSpec = deployUnit.has_replica_set()
                    ? deployUnit.replica_set().replica_set_template().pod_template_spec()
                    : deployUnit.multi_cluster_replica_set().replica_set().pod_template_spec();
                ValidatePodAgentSpec(podTemplateSpec.spec().pod_agent_payload().spec());

                if (deployUnit.has_tvm_config()) {
                    ValidateTvmConfig(deployUnit.tvm_config());
                }
            }
        } catch (const std::exception& ex) {
            ThrowValidationError(ex, NClient::NApi::EErrorCode::InvalidObjectSpec, stage->GetId());
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
};

std::unique_ptr<IObjectTypeHandler> CreateStageTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TStageTypeHandler(bootstrap));
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
    if (!re2::RE2::FullMatch(id, stageIdPattern)) {
        THROW_ERROR_EXCEPTION("%v %Qv must match regexp %Qv",
            description,
            id,
            stageIdPattern.Pattern());
    }
}

void ValidatePodAgentSpec(const NInfra::NPodAgent::API::TPodAgentSpec& spec)
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
    if (!re2::RE2::FullMatch(id, podAgentObjectIdPattern)) {
        THROW_ERROR_EXCEPTION("%v %Qv must match regexp %Qv",
            description,
            id,
            podAgentObjectIdPattern.Pattern());
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

