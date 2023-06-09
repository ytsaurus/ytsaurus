#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/library/auth/auth.h>

namespace NYT::NYqlAgent {

using namespace NSecurityClient;
using namespace NAuth;

////////////////////////////////////////////////////////////////////////////////

void TYqlPluginConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mr_job_binary", &TThis::MRJobBinary)
        .Default("./mrjob");
    registrar.Parameter("udf_directory", &TThis::UdfDirectory)
        .Default();
    registrar.Parameter("additional_clusters", &TThis::AdditionalClusters)
        .Default();
    registrar.Parameter("yt_token", &TThis::YTToken)
        .Default();
    registrar.Parameter("yql_plugin_shared_library", &TThis::YqlPluginSharedLibrary)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("yql_thread_count", &TThis::YqlThreadCount)
        .Default(256);
}

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentDynamicConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TYqlAgentServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("yql_agent", &TThis::YqlAgent)
        .DefaultNew();
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("user", &TThis::User)
        .Default(YqlAgentUserName);
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
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
