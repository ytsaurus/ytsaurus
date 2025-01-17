#include "program.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/node/cellar_node/config.h>
#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/node/tablet_node/serialize.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/hydra/dry_run/helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NClusterNode {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeProgram
    : public TServerProgram<TClusterNodeProgramConfig, TClusterNodeDynamicConfig>
{
public:
    TClusterNodeProgram()
    {
        Opts_
            .AddLongOption(
                "dump-snapshot",
                "Dumps tablet cell snapshot\n"
                "Expects path to snapshot")
            .Handler0([&] { DumpSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "validate-snapshot",
                "Loads tablet cell snapshot in a dry run mode\n"
                "Expects path to snapshot")
            .Handler0([&] { ValidateSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "snapshot-meta",
                "YSON-serialized snapshot meta")
            .Handler0([&] { SnapshotMetaFlag_ = true; })
            .StoreMappedResultT<TString>(&SnapshotMeta_, &CheckYsonArgMapper)
            .RequiredArgument("PATH");
        Opts_
            .AddLongOption(
                "cell-id",
                "Tablet cell id")
            .Handler0([&] { CellIdFlag_ = true; })
            .StoreMappedResultT<TString>(&CellId_, &CheckGuidArgMapper)
            .RequiredArgument("CELL ID");
        Opts_
            .AddLongOption(
                "tablet-cell-bundle",
                "Cell's tablet cell bundle name")
            .StoreResult(&TabletCellBundle_)
            .RequiredArgument("STRING");
        Opts_
            .AddLongOption(
                "clock-cluster-tag",
                "Tablet cell clock cluster tag")
            .StoreResult(&ClockClusterTag_)
            .Optional();
        Opts_
            .AddLongOption(
                "replay-changelogs",
                "Replays one or more consecutive tablet cell changelogs from a given directory\n"
                "Expects space-separated paths to changelogs\n"
                "Typically used in conjunction with 'validate-snapshot' option to apply changelogs over a specific snapshot")
            .Handler0([&] { ReplayChangelogsFlag_ = true; })
            .SplitHandler(&ReplayChangelogsPaths_, ' ')
            .RequiredArgument("DIRECTORY");
        Opts_
            .AddLongOption(
                "build-snapshot",
                "Saves final state into snapshot\n"
                "Expects path to snapshot directory")
            .Handler0([&] { BuildSnapshotFlag_ = true; })
            .StoreMappedResult(&BuildSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("DIRECTORY");
        Opts_
            .AddLongOption(
                "skip-tvm-service-env-validation",
                "Do not validate TVM service files")
            .SetFlag(&SkipTvmServiceEnvValidationFlag_)
            .NoArgument();

        SetMainThreadName("NodeProg");
    }

private:
    bool IsDumpSnapshotMode() const
    {
        return DumpSnapshotFlag_;
    }

    bool IsValidateSnapshotMode() const
    {
        return ValidateSnapshotFlag_;
    }

    bool IsLoadSnapshotMode() const
    {
        return IsDumpSnapshotMode() || IsValidateSnapshotMode();
    }

    bool IsReplayChangelogsMode() const
    {
        return ReplayChangelogsFlag_;
    }

    bool IsBuildSnapshotMode() const
    {
        return BuildSnapshotFlag_;
    }

    bool IsDryRunMode() const
    {
        return
            IsLoadSnapshotMode() ||
            IsReplayChangelogsMode() ||
            IsBuildSnapshotMode();
    }

    void ValidateOpts() final
    {
        if (IsReplayChangelogsMode() && !(CellIdFlag_ && SnapshotMetaFlag_)) {
            THROW_ERROR_EXCEPTION("Option 'replay-changelog' can only be used when options 'cell-id' and 'snapshot-meta' are present");
        }

        if (IsDumpSnapshotMode() && IsValidateSnapshotMode()) {
            THROW_ERROR_EXCEPTION("Options 'dump-snapshot' and 'validate-snapshot' are mutually exclusive");
        }

        if (IsDumpSnapshotMode() && IsReplayChangelogsMode()) {
            THROW_ERROR_EXCEPTION("Option 'replay-changelogs' can not be used with 'dump-snapshot'");
        }

        if (IsBuildSnapshotMode() && !IsReplayChangelogsMode() && !IsValidateSnapshotMode()) {
            THROW_ERROR_EXCEPTION("Option 'build-snapshot' can only be used with 'validate-snapshot' or 'replay-changelog'");
        }

        if (IsReplayChangelogsMode()) {
            auto changelogDirectory = NFS::GetDirectoryName(ReplayChangelogsPaths_.front());
            for (const auto& fileName : ReplayChangelogsPaths_) {
                THROW_ERROR_EXCEPTION_IF(
                    changelogDirectory != NFS::GetDirectoryName(fileName),
                    "Changelogs must be located in one directory");
            }
        }
    }

    void TweakConfig() final
    {
        auto config = GetConfig();

        if (IsDryRunMode()) {
            auto loggingConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
            loggingConfig->ShutdownGraceTimeout = TDuration::Seconds(10);

            auto localSnapshotStoreConfig = New<NHydra::TLocalSnapshotStoreConfig>();
            localSnapshotStoreConfig->Path = NFS::GetRealPath(BuildSnapshotPath_);
            localSnapshotStoreConfig->UseHeaderlessWriter = true;

            YT_VERIFY(localSnapshotStoreConfig->StoreType == NHydra::ESnapshotStoreType::Local);
            config->TabletNode->Snapshots  = localSnapshotStoreConfig;

            if (SkipTvmServiceEnvValidationFlag_) {
                auto authManagerConfig = config->GetSingletonConfig<NAuth::TNativeAuthenticationManagerConfig>();
                authManagerConfig->EnableValidation = false;
                authManagerConfig->EnableSubmission = false;
                authManagerConfig->TvmService = nullptr;
            }

            config->TabletNode->ResourceLimits->Slots = std::max(config->TabletNode->ResourceLimits->Slots, 1);

            config->DryRun->EnableDryRun = true;
            config->DryRun->TabletCellId = CellIdFlag_
                ? CellId_
                : MakeWellKnownId(EObjectType::TabletCell, TCellTag(1));
            config->DryRun->TabletCellBundle = TabletCellBundle_;
            config->DryRun->ClockClusterTag = TCellTag(ClockClusterTag_);

            const auto& cellarManager = config->CellarNode->CellarManager;
            for (const auto& [type, cellarConfig] : cellarManager->Cellars) {
                cellarConfig->Occupant->EnableDryRun = true;
                cellarConfig->Occupant->Snapshots = localSnapshotStoreConfig;
            }
        }

        if (IsDumpSnapshotMode()) {
            config->SetSingletonConfig(NLogging::TLogManagerConfig::CreateSilent());
        }

        if (IsValidateSnapshotMode()) {
            config->SetSingletonConfig(NHydra::CreateDryRunLoggingConfig());
        }
    }

    void DoStart() final
    {
        // TODO(babenko): refactor
        ConfigureAllocator({.SnapshotUpdatePeriod = GetConfig()->HeapProfiler->SnapshotUpdatePeriod});

        auto bootstrap = CreateNodeBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);

        if (IsDryRunMode()) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();

            bootstrap->Initialize();

            const auto& cellarNodeBootstrap = bootstrap->GetCellarNodeBootstrap();

            if (IsLoadSnapshotMode()) {
                auto meta = SnapshotMeta_
                    ? NYTree::ConvertTo<NHydra::NProto::TSnapshotMeta>(SnapshotMeta_)
                    : NHydra::NProto::TSnapshotMeta{};

                cellarNodeBootstrap->LoadSnapshot(LoadSnapshotPath_, meta, IsDumpSnapshotMode());
            }

            if (IsReplayChangelogsMode()) {
                cellarNodeBootstrap->ReplayChangelogs(ReplayChangelogsPaths_);
            }

            if (IsBuildSnapshotMode()) {
                cellarNodeBootstrap->BuildSnapshot();
            }

            cellarNodeBootstrap->FinishDryRun();

            // XXX(babenko): ASAN complains about memory leak on graceful exit.
            // Must try to resolve them later.
            AbortProcessSilently(EProcessExitCode::OK);
        }

        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }

private:
    bool DumpSnapshotFlag_ = false;
    bool ValidateSnapshotFlag_ = false;
    TString LoadSnapshotPath_;
    bool ReplayChangelogsFlag_ = false;
    std::vector<TString> ReplayChangelogsPaths_;
    bool CellIdFlag_ = false;
    TCellId CellId_ = MakeWellKnownId(EObjectType::TabletCell, TCellTag(1));
    TString TabletCellBundle_ = "fake-bundle";
    TCellTag::TUnderlying ClockClusterTag_ = InvalidCellTag.Underlying();
    bool BuildSnapshotFlag_ = false;
    TString BuildSnapshotPath_;
    bool SnapshotMetaFlag_ = false;
    NYson::TYsonString SnapshotMeta_;
    bool SkipTvmServiceEnvValidationFlag_ = false;
};

////////////////////////////////////////////////////////////////////////////////

void RunClusterNodeProgram(int argc, const char** argv)
{
    TClusterNodeProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
