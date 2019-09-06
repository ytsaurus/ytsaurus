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
            ->SetExtensible();

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
                for (const auto& workload : podTemplateSpec.spec().pod_agent_payload().spec().workloads()) {
                    ValidatePodAgentWorkloadEnv(workload);
                }
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

void ValidatePodAgentWorkloadEnv(const NInfra::NPodAgent::API::TWorkload& workload)
{
    THashSet<TString> nameSet;
    for (const auto& env : workload.env()) {
        if (!nameSet.insert(env.name()).second) {
            THROW_ERROR_EXCEPTION("Multiple entries for environment variable %Qv in workload %Qv",
                env.name(),
                workload.id());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

