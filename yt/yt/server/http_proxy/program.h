#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/logging/program_describe_structured_logs_mixin.h>

#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/core/json/json_parser.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <util/system/thread.h>

namespace NYT::NHttpProxy {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class THttpProxyProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NHttpProxy::TProxyConfig>
    , public NLogging::TProgramDescribeStructuredLogsMixin
{
public:
    THttpProxyProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_, false)
        , TProgramDescribeStructuredLogsMixin(Opts_)
    {
        Opts_
            .AddLongOption(
                "remote-cluster-proxy",
                "if set, proxy would download cluster connection from //sys/@cluster_connection "
                "on cluster CLUSTER using http interface and then run as an unexposed local proxy "
                "for CLUSTER; if port is not specified, .yt.yandex.net:80 will be assumed automatically; "
                "proxy will be run with default http proxy config on port 8080, but config patch may be "
                "provided via --config option")
            .StoreResult(&RemoteClusterProxy_)
            .RequiredArgument("CLUSTER")
            .Optional();
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        TThread::SetCurrentThreadName("ProxyMain");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();

        if (HandleSetsidOptions()) {
            return;
        }
        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        if (HandleDescribeStructuredLogsOptions()) {
            return;
        }

        NHttpProxy::TProxyConfigPtr config;
        NYTree::INodePtr configNode;
        if (RemoteClusterProxy_) {
            // Form a default http proxy config listening port 8080.
            auto defaultConfig = New<NHttpProxy::TProxyConfig>();
            defaultConfig->SetDefaults();
            defaultConfig->Port = 8080;
            defaultConfig->MonitoringPort = 10013;
            auto shardConfig = New<NProfiling::TShardConfig>();
            shardConfig->Filter = {"yt/"};
            defaultConfig->SolomonExporter->Shards = {{"default", std::move(shardConfig)}};
            defaultConfig->ClusterConnection = New<NApi::NNative::TConnectionCompoundConfig>();
            defaultConfig->ClusterConnection->Static = New<NApi::NNative::TConnectionStaticConfig>();
            defaultConfig->ClusterConnection->Dynamic = New<NApi::NNative::TConnectionDynamicConfig>();
            defaultConfig->Logging = NLogging::TLogManagerConfig::CreateYTServer("http_proxy" /*componentName*/);
            // One may disable authentication at all via config, but by default it is better
            // to require authentication. Even YT developers may unintentionally do something
            // harmful, in which case we do not want to see requests under root in cluster logs.
            defaultConfig->Auth->BlackboxTokenAuthenticator = New<NAuth::TCachingBlackboxTokenAuthenticatorConfig>();
            defaultConfig->Auth->BlackboxTokenAuthenticator->Scope = "yt:api";
            defaultConfig->Driver = New<NDriver::TDriverConfig>();
            // Dump it into node and apply patch from config file (if present).
            configNode = NYTree::ConvertToNode(defaultConfig);
            if (auto configNodePatch = GetConfigNode(true /*returnNullIfNotSupplied*/)) {
                configNode = NYTree::PatchNode(configNode, configNodePatch);
            }
            // Finally load it back.
            config = New<NHttpProxy::TProxyConfig>();
            config->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
            config->Load(configNode);
        } else {
            config = GetConfig();
            configNode = GetConfigNode();
        }

        ConfigureAllocator({.SnapshotUpdatePeriod = config->HeapProfiler->SnapshotUpdatePeriod});

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        if (RemoteClusterProxy_) {
            // Set http proxy cluster connection.
            auto clusterConnectionNode = DownloadClusterConnection(RemoteClusterProxy_, NHttpProxy::HttpProxyLogger);
            config->ClusterConnection = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnectionNode);
        }

        auto bootstrap = New<NHttpProxy::TBootstrap>(std::move(config), std::move(configNode));
        bootstrap->Run();
    }

private:
    TString RemoteClusterProxy_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
