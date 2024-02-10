#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NRpcProxy::TProxyConfig>
{
public:
    TRpcProxyProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_
            .AddLongOption(
                "remote-cluster-proxy",
                "if set, proxy would download cluster connection from //sys/@cluster_connection "
                "on cluster CLUSTER using http interface and then run as an unexposed local proxy "
                "for CLUSTER; if port is not specified, .yt.yandex.net:80 will be assumed automatically; "
                "proxy will be run with default rpc proxy config on port 9013, but config patch may be "
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

        NRpcProxy::TProxyConfigPtr config;
        NYTree::INodePtr configNode;
        if (RemoteClusterProxy_) {
            // Form a default rpc proxy config listening port 9013.
            auto defaultConfig = New<NRpcProxy::TProxyConfig>();
            defaultConfig->SetDefaults();
            defaultConfig->ClusterConnection = New<NApi::NNative::TConnectionCompoundConfig>();
            defaultConfig->ClusterConnection->Static = New<NApi::NNative::TConnectionStaticConfig>();
            defaultConfig->ClusterConnection->Dynamic = New<NApi::NNative::TConnectionDynamicConfig>();
            defaultConfig->Logging = NLogging::TLogManagerConfig::CreateYTServer(
                /*componentName*/ "rpc_proxy",
                /*directory*/ ".",
                /*structuredCategoryToWriterName*/ {{"RpcProxyStructuredMain", "main"}, {"RpcProxyStructuredError", "error"}});
            // One may disable authentication at all via config, but by default it is better
            // to require authentication. Even YT developers may unintentionally do something
            // harmful, in which case we do not want to see requests under root in cluster logs.
            defaultConfig->BlackboxTokenAuthenticator = New<NAuth::TCachingBlackboxTokenAuthenticatorConfig>();
            defaultConfig->BlackboxTokenAuthenticator->Scope = "yt:api";
            // Dump it into node and apply patch from config file (if present).
            configNode = NYTree::ConvertToNode(defaultConfig);
            if (auto configNodePatch = GetConfigNode(true /*returnNullIfNotSupplied*/)) {
                configNode = NYTree::PatchNode(configNode, configNodePatch);
            }
            // Finally load it back.
            config = New<NRpcProxy::TProxyConfig>();
            config->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
            config->Load(configNode);

            if (!config->RpcPort) {
                config->RpcPort = 9013;
                config->Postprocess();
            }

            // Do not forget to forcefully disable discovery service. Otherwise our local proxy would register
            // as a regular proxy and start serving user requests.
            config->DiscoveryService->Enable = false;
        } else {
            config = GetConfig();
            configNode = GetConfigNode();
        }

        ConfigureAllocator({.SnapshotUpdatePeriod = config->HeapProfiler->SnapshotUpdatePeriod});

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        if (RemoteClusterProxy_) {
            auto clusterConnectionNode = DownloadClusterConnection(RemoteClusterProxy_, RpcProxyLogger);
            config->ClusterConnection = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnectionNode);
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NRpcProxy::TBootstrap(std::move(config), std::move(configNode));
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
    }

private:
    TString RemoteClusterProxy_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
