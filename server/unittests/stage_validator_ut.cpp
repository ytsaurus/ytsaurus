#include <yt/core/test_framework/framework.h>

#include <yp/server/objects/stage.h>
#include <yp/server/objects/stage_type_handler.h>

namespace NYP::NServer::NObjects::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

NInfra::NPodAgent::API::TEnvVar LiteralEnvVar(const TString& name, const TString& value)
{
    NInfra::NPodAgent::API::TEnvVar result;
    result.set_name(name);
    result.mutable_value()->mutable_literal_env()->set_value(value);
    return result;
}

TEST(StageValidator, ValidateIdTest)
{
    ASSERT_THROW(ValidateStageAndDeployUnitId("inv*", "Stage id"), TErrorException);

    ValidateStageAndDeployUnitId("valid", "Stage id");
}

TEST(StageValidator, ValidateEnvTest)
{
    NInfra::NPodAgent::API::TWorkload workload;
    workload.set_id("id");
    *workload.add_env() = LiteralEnvVar("NAME", "value1");
    *workload.add_env() = LiteralEnvVar("NAME", "value2");
    ASSERT_THROW(ValidatePodAgentWorkloadEnv(workload), TErrorException);

    workload.clear_env();
    ValidatePodAgentWorkloadEnv(workload);
}

TEST(StageValidator, ValidateTvmConfigTest)
{
    NClient::NApi::NProto::TTvmConfig config;
    config.set_mode(NClient::NApi::NProto::TTvmConfig_EMode_ENABLED);
    config.add_clients()->mutable_source()->set_alias("alias_source");
    config.add_clients()->mutable_source()->set_alias("alias_source");
    ASSERT_THROW(ValidateTvmConfig(config), TErrorException);

    config.clear_clients();
    config.add_clients()->mutable_source()->set_alias("alias_source");
    config.mutable_clients(0)->mutable_source()->set_app_id(137);
    config.mutable_clients(0)->add_destinations()->set_alias("alias_destination");
    config.mutable_clients(0)->add_destinations()->set_alias("alias_destination");
    ASSERT_THROW(ValidateTvmConfig(config), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NObjects::NTests
