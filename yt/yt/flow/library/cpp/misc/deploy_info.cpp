#include "deploy_info.h"

#include "deploy_url_provider.h"

#include <cstdlib>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TYDeployInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("stage_id", &TThis::StageId)
        .Default();
    registrar.Parameter("stage_url", &TThis::StageUrl)
        .Default()
        .DontSerializeDefault();
}

void TVanillaDeployInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("cluster", &TThis::Cluster)
        .Default();
    registrar.Parameter("operation_id", &TThis::OperationId)
        .Default();
    registrar.Parameter("job_id", &TThis::JobId)
        .Default();
}

void TDeployInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EDeployType::Ydeploy);
    registrar.Parameter("ydeploy", &TThis::YDeploy)
        .Default()
        .DontSerializeDefault();
    registrar.Parameter("vanilla", &TThis::Vanilla)
        .Default()
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string GetEnvOrEmpty(const char* name)
{
    const char* value = std::getenv(name);
    return value ? std::string(value) : std::string();
}

} // namespace

TDeployInfoPtr MakeDeployInfo()
{
    // A vanilla job environment is set by the YT job proxy, so it is checked first.
    if (auto operationId = GetEnvOrEmpty("YT_OPERATION_ID"); !operationId.empty()) {
        auto vanilla = New<TVanillaDeployInfo>();
        vanilla->Cluster = GetEnvOrEmpty("YT_CLUSTER_NAME");
        vanilla->OperationId = std::move(operationId);
        vanilla->JobId = GetEnvOrEmpty("YT_JOB_ID");

        auto info = New<TDeployInfo>();
        info->Type = EDeployType::Vanilla;
        info->Vanilla = std::move(vanilla);
        return info;
    }

    if (auto stageId = GetEnvOrEmpty("DEPLOY_STAGE_ID"); !stageId.empty()) {
        auto yDeploy = New<TYDeployInfo>();
        yDeploy->StageId = std::move(stageId);
        yDeploy->StageUrl = GetDeployStageUrl();

        auto info = New<TDeployInfo>();
        info->Type = EDeployType::Ydeploy;
        info->YDeploy = std::move(yDeploy);
        return info;
    }

    return nullptr;
}

const TDeployInfoPtr& GetDeployInfo()
{
    static const TDeployInfoPtr cached = MakeDeployInfo();
    return cached;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
