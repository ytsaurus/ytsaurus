#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/private.h>

#include <yt/yt/server/node/cellar_node/config.h>
#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/node/tablet_node/serialize.h>

#include <yt/yt/server/lib/misc/cluster_connection.h>

#include <yt/yt/server/lib/hydra/dry_run/utils.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <util/system/compiler.h>
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
            .AddLongOption("dump-snapshot", "dump tablet cell snapshot and exit")
            .StoreMappedResult(&SnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("validate-snapshot", "load tablet cell snapshot in a dry run mode")
            .StoreMappedResult(&SnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("snapshot-meta", "YSON-serialized snapshot meta")
            .StoreMappedResultT<TString>(&DryRunSnapshotMeta_, &CheckYsonArgMapper)
            .RequiredArgument("YSON");
        Opts_
            .AddLongOption("cell-id", "tablet cell id")
            .StoreResult(&CellId_)
            .RequiredArgument("CELL ID");
        Opts_
            .AddLongOption("clock-cluster-tag", "tablet cell clock cluster tag")
            .StoreResult(&ClockClusterTag_)
            .Optional();
        Opts_
            .AddLongOption("replay-changelogs", "replay one or more consecutive tablet cell changelogs\n"
                           "Usually used in conjunction with 'validate-snapshot' option to apply changelogs over a specific snapshot")
            .SplitHandler(&ChangelogFileNames_, ' ')
            .RequiredArgument("CHANGELOG");
        Opts_
            .AddLongOption("build-snapshot", "save resulting tablet cell state to a snapshot\n"
                                             "Path to a directory can be specified here\n"
                                             "By default snapshot will be saved in the working directory")
            .StoreResult(&SnapshotBuildDirectory_)
            .RequiredArgument("DIRECTORY");
        Opts_
            .AddLongOption(
                "remote-cluster-proxy",
                "if set, cluster connection will be downloaded from //sys/@cluster_connection "
                "and tablet node will be run. ")
            .StoreResult(&RemoteClusterProxy_)
            .RequiredArgument("CLUSTER")
            .Optional();
        Opts_
            .AddLongOption("skip-tvm-service-env-validation", "don't validate TVM service files")
            .SetFlag(&SkipTvmServiceEnvValidation_)
            .NoArgument();
        Opts_
            .AddLongOption("sleep-after-initialize", "sleep for 10s after calling TBootstrap::Initialize()")
            .SetFlag(&SleepAfterInitialize_)
            .NoArgument();
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("NodeMain");

        auto dumpSnapshot = parseResult.Has("dump-snapshot");
        auto validateSnapshot = parseResult.Has("validate-snapshot");
        auto snapshotMeta = parseResult.Has("snapshot-meta");
        auto cellId = parseResult.Has("cell-id");
        auto replayChangelogs = parseResult.Has("replay-changelogs");
        auto buildSnapshot = parseResult.Has("build-snapshot");

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

        auto loadSnapshot = dumpSnapshot || validateSnapshot;
        auto isDryRun = loadSnapshot || replayChangelogs || buildSnapshot;

        if (replayChangelogs && !(cellId && snapshotMeta)) {
            THROW_ERROR_EXCEPTION("Option 'replay-changelog' can only be used when options 'cell-id' and 'snapshot-meta' are present");
        }

        if (dumpSnapshot && validateSnapshot) {
            THROW_ERROR_EXCEPTION("Options 'dump-snapshot' and 'validate-snapshot' are mutually exclusive");
        }

        if (dumpSnapshot && replayChangelogs) {
            THROW_ERROR_EXCEPTION("Option 'replay-changelogs' can not be used with 'dump-snapshot'");
        }

        if (buildSnapshot && !replayChangelogs && !validateSnapshot) {
            THROW_ERROR_EXCEPTION("Option 'build-snapshot' can only be used with 'validate-snapshot' or 'replay-changelog'");
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

        if (isDryRun) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();

            config->Logging->ShutdownGraceTimeout = TDuration::Seconds(10);

            auto localSnapshotStoreConfig = New<NHydra::TLocalSnapshotStoreConfig>();
            localSnapshotStoreConfig->Path = buildSnapshot
                ? NFS::GetRealPath(SnapshotBuildDirectory_)
                : NFS::GetRealPath(".");
            localSnapshotStoreConfig->UseHeaderlessWriter = true;

            YT_VERIFY(localSnapshotStoreConfig->StoreType == NHydra::ESnapshotStoreType::Local);
            config->TabletNode->Snapshots  = localSnapshotStoreConfig;

            if (SkipTvmServiceEnvValidation_) {
                const auto& nativeAuthenticationManager = config->NativeAuthenticationManager;
                nativeAuthenticationManager->EnableValidation = false;
                nativeAuthenticationManager->EnableSubmission = false;
                nativeAuthenticationManager->TvmService = nullptr;
            }

            config->TabletNode->ResourceLimits->Slots = std::max(config->TabletNode->ResourceLimits->Slots, 1);

            config->DryRun->EnableDryRun = true;
            config->DryRun->TabletCellId = CellId_
                ? TGuid::FromString(CellId_)
                : NObjectClient::MakeWellKnownId(
                    NObjectClient::EObjectType::TabletCell,
                    NObjectClient::TCellTag(1));
            config->DryRun->ClockClusterTag = ClockClusterTag_
                ? NApi::TClusterTag(*ClockClusterTag_)
                : NObjectClient::InvalidCellTag;

            const auto& cellarManager = config->CellarNode->CellarManager;
            for (auto& [type, cellarConfig] : cellarManager->Cellars) {
                cellarConfig->Occupant->EnableDryRun = true;
                cellarConfig->Occupant->Snapshots = localSnapshotStoreConfig;
            }
        }

        if (dumpSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateSilent();
        } else if (validateSnapshot) {
            NHydra::ConfigureDryRunLogging(config);
        }

        if (replayChangelogs) {
            auto changelogDirectory = NFS::GetDirectoryName(ChangelogFileNames_.front());
            for (const auto& fileName : ChangelogFileNames_) {
                THROW_ERROR_EXCEPTION_IF(
                    changelogDirectory != NFS::GetDirectoryName(fileName),
                    "Changelogs must be located in one directory");
            }
        }

        ConfigureAllocator({.SnapshotUpdatePeriod = config->HeapProfiler->SnapshotUpdatePeriod});
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
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();

        if (SleepAfterInitialize_) {
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(10));
        }

        if (!isDryRun) {
            bootstrap->Run();
        } else {
            const auto& cellarNodeBootstrap = bootstrap->GetCellarNodeBootstrap();
            if (loadSnapshot) {
                auto meta = DryRunSnapshotMeta_
                    ? NYTree::ConvertTo<NHydra::NProto::TSnapshotMeta>(DryRunSnapshotMeta_)
                    : NHydra::NProto::TSnapshotMeta{};

                cellarNodeBootstrap->LoadSnapshotOrThrow(SnapshotPath_, meta, dumpSnapshot);
            }
            if (replayChangelogs) {
                cellarNodeBootstrap->ReplayChangelogsOrThrow(std::move(ChangelogFileNames_));
            }
            if (buildSnapshot) {
                cellarNodeBootstrap->BuildSnapshotOrThrow();
            }
            cellarNodeBootstrap->FinishDryRunOrThrow();
        }

        // XXX(babenko): ASAN complains about memory leak on graceful exit.
        // Must try to resolve them later.
        _exit(0);
    }

private:
    TString SnapshotPath_;
    std::vector<TString> ChangelogFileNames_;
    TString CellId_;
    std::optional<ui16> ClockClusterTag_;
    TString SnapshotBuildDirectory_;
    NYson::TYsonString DryRunSnapshotMeta_;
    TString RemoteClusterProxy_;
    bool SkipTvmServiceEnvValidation_ = false;
    bool SleepAfterInitialize_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
