#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yql/plugin/process/config.h>

#include <util/string/vector.h>

#include <array>
#include <utility>

namespace NYT::NYqlAgent {

using namespace NSecurityClient;
using namespace NAuth;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("token_expiration_timeout", &TThis::TokenExpirationTimeout)
        .Default(TDuration::Minutes(20));
    registrar.Parameter("refresh_token_period", &TThis::RefreshTokenPeriod)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("issue_token_attempts", &TThis::IssueTokenAttempts)
        .Default(10);
    registrar.Parameter("yql_thread_count", &TThis::YqlThreadCount)
        .Default(256);
    registrar.Parameter("max_supported_yql_version", &TThis::MaxSupportedYqlVersion)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_simultaneous_queries", &TThis::MaxSimultaneousQueries)
        .Default(128);
    registrar.Parameter("state_check_period", &TThis::StateCheckPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("gateways", &TThis::GatewaysConfig)
        .Default(GetEphemeralNodeFactory()->CreateMap())
        .ResetOnLoad();
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yql_agent", &TThis::YqlAgent)
        .DefaultNew();
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("user", &TThis::User)
        // TODO(babenko): migrate to std::string
        .Default(TString(YqlAgentUserName));
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("root", &TThis::Root)
        .Default("//sys/yql_agent");
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (auto& dynamicConfigPath = config->DynamicConfigPath; dynamicConfigPath.empty()) {
            dynamicConfigPath = config->Root + "/config";
        }
    });
};

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentServerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yql_agent", &TThis::YqlAgent)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
