#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/private.h>

#include <yt/yt/server/node/tablet_node/serialize.h>

#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <util/system/thread.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NClusterNode::TClusterNodeConfig, NClusterNode::TClusterNodeDynamicConfig>
{
public:
    TClusterNodeProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_, false)
    {
        Opts_
            .AddLongOption("validate-snapshot")
            .StoreMappedResult(&ValidateSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "remote-cluster-proxy",
                "if set, cluster connection will be downloaded from //sys/@cluster_connection "
                "and tablet node will be run. ")
            .StoreResult(&RemoteClusterProxy_)
            .RequiredArgument("CLUSTER")
            .Optional();
        Opts_
            .AddLongOption("sleep-after-initialize", "sleep for 10s after calling TBootstrap::Initialize()")
            .SetFlag(&SleepAfterInitialize_)
            .NoArgument();
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        TThread::SetCurrentThreadName("NodeMain");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        ConfigureAllocator({});

        if (HandleSetsidOptions()) {
            return;
        }
        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        TClusterNodeConfigPtr config;
        NYTree::INodePtr configNode;
        if (RemoteClusterProxy_) {
            // Form a default cluster node config.
            auto defaultConfig = New<TClusterNodeConfig>();
            defaultConfig->Logging = NLogging::TLogManagerConfig::CreateYTServer(/*componentName*/ "cluster_node");
            defaultConfig->RpcPort = 9012;

            // Increase memory limits.
            defaultConfig->ResourceLimits->TotalMemory = 30_GB;

            // Create a tablet node config with 5 tablet slots.
            defaultConfig->Flavors = {NNodeTrackerClient::ENodeFlavor::Tablet};
            auto& tabletCellarConfig = defaultConfig->CellarNode->CellarManager->Cellars[NCellarClient::ECellarType::Tablet];
            tabletCellarConfig = New<NCellarAgent::TCellarConfig>();
            tabletCellarConfig->Size = 5;

            defaultConfig->ClusterConnection = New<NApi::NNative::TConnectionCompoundConfig>();

            // Dump it into node and apply patch from config file (if present).
            configNode = NYTree::ConvertToNode(defaultConfig);
            if (auto configNodePatch = GetConfigNode(/*returnNullIfNotSupplied*/ true)) {
                configNode = NYTree::PatchNode(configNode, configNodePatch);
            }
            // Finally load it back.
            config = New<TClusterNodeConfig>();
            config->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
            config->Load(configNode);

            // WARNING: Changing cell reign in local mode is a very bad idea. Think twice before doing it!
            NTabletNode::SetReignChangeAllowed(/*allowed*/ false);
        } else {
            config = GetConfig();
            configNode = GetConfigNode();
        }

        if (ValidateSnapshot_) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();

            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();
        }

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        if (RemoteClusterProxy_) {
            // Set controller agent cluster connection.
            auto clusterConnectionNode = DownloadClusterConnection(RemoteClusterProxy_, ClusterNodeLogger);
            config->ClusterConnection = ConvertTo<NApi::NNative::TConnectionCompoundConfigPtr>(clusterConnectionNode);
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = CreateBootstrap(std::move(config), std::move(configNode)).release();
        bootstrap->Initialize();

        if (SleepAfterInitialize_) {
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(10));
        }

        if (ValidateSnapshot_) {
            bootstrap->ValidateSnapshot(ValidateSnapshot_);

            // XXX(babenko): ASAN complains about memory leak on graceful exit.
            // Must try to resolve them later.
            _exit(0);
        } else {
            bootstrap->Run();
        }
    }

private:
    TString ValidateSnapshot_;
    TString RemoteClusterProxy_;
    bool SleepAfterInitialize_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
