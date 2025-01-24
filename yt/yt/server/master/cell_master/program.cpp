#include "program.h"

#include "bootstrap.h"
#include "config.h"
#include "serialize.h"
#include "snapshot_exporter.h"

#include <yt/yt/server/lib/hydra/dry_run/helpers.h>

#include <yt/yt/ytlib/auth/config.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/logging/config.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterProgram
    : public TServerProgram<TCellMasterProgramConfig>
{
public:
    TCellMasterProgram()
    {
        Opts_
            .AddLongOption(
                "dump-snapshot",
                "Dumps master snapshot\n"
                "Expects path to snapshot")
            .Handler0([&] { DumpSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "snapshot-dump-mode",
                "Valid options are: content, checksum")
            .StoreMappedResultT<TStringBuf>(&SnapshotDumpMode_, &ParseEnumArgMapper<ESerializationDumpMode>)
            .RequiredArgument("MODE");
        Opts_
            .AddLongOption(
                "export-snapshot",
                "Exports master snapshot\n"
                "Expects path to snapshot")
            .Handler0([&] { ExportSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "export-config",
                "Path to config file (in YSON format) for master snapshot export")
            .Handler0([&] { ExportConfigFlag_ = true; })
            .StoreResult(&ExportConfigPath_)
            .RequiredArgument("FILE");
        Opts_
            .AddLongOption(
                "validate-snapshot",
                "Loads master snapshot in a dry run mode\n"
                "Expects path to snapshot")
            .Handler0([&] { ValidateSnapshotFlag_ = true; })
            .StoreMappedResult(&LoadSnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption(
                "replay-changelogs",
                "Replays one or more consecutive master changelogs\n"
                "Expects space-separated paths to changelogs\n"
                "Typically used in conjunction with 'validate-snapshot' option to apply changelogs over a specific snapshot")
            .Handler0([&] { ReplayChangelogsFlag_ = true; })
            .SplitHandler(&ReplayChangelogsPaths_, ' ')
            .RequiredArgument("CHANGELOG");
        Opts_
            .AddLongOption(
                "build-snapshot",
                "Saves final state into snapshot\n"
                "Expects path to snapshot directory")
            .Handler0([&] { BuildSnapshotFlag_ = true; })
            .StoreResult(&BuildSnapshotPath_)
            .RequiredArgument("DIRECTORY");
        Opts_
            .AddLongOption(
                "skip-tvm-service-env-validation",
                "Do not validate TVM service files")
            .SetFlag(&SkipTvmServiceEnvValidationFlag_)
            .NoArgument();
        Opts_
            .AddLongOption(
                "compatibility-info",
                "Prints master binary compatibility info")
            .SetFlag(&PrintCompatibilityInfoFlag_)
            .NoArgument();

        SetMainThreadName("MasterProg");
    }

private:
    bool DumpSnapshotFlag_ = false;
    ESerializationDumpMode SnapshotDumpMode_ = ESerializationDumpMode::Content;
    bool ValidateSnapshotFlag_ = false;
    bool ExportSnapshotFlag_ = false;
    TString LoadSnapshotPath_;
    bool ExportConfigFlag_ = false;
    TString ExportConfigPath_;
    bool ReplayChangelogsFlag_ = false;
    std::vector<TString> ReplayChangelogsPaths_;
    bool BuildSnapshotFlag_ = false;
    TString BuildSnapshotPath_;
    bool SkipTvmServiceEnvValidationFlag_ = false;
    bool PrintCompatibilityInfoFlag_ = false;

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
        return IsDumpSnapshotMode() || IsValidateSnapshotMode() || IsExportSnapshotMode();
    }

    bool IsExportSnapshotMode() const
    {
        return ExportSnapshotFlag_;
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

    bool IsPrintCompatibilityInfoMode() const
    {
        return PrintCompatibilityInfoFlag_;
    }

    void ValidateOpts() final
    {
        if (static_cast<int>(IsDumpSnapshotMode()) +
            static_cast<int>(IsValidateSnapshotMode()) +
            static_cast<int>(IsExportSnapshotMode()) +
            static_cast<int>(IsPrintCompatibilityInfoMode()) > 1)
        {
            THROW_ERROR_EXCEPTION("Options 'dump-snapshot', 'validate-snapshot', 'export-snapshot' and 'compatibility-info' are mutually exclusive");
        }

        if ((IsDumpSnapshotMode() || IsExportSnapshotMode()) && IsReplayChangelogsMode()) {
            THROW_ERROR_EXCEPTION("Option 'replay-changelogs' can not be used with 'dump-snapshot' or 'export-snapshot'");
        }

        if (IsBuildSnapshotMode() && !IsReplayChangelogsMode() && !IsValidateSnapshotMode()) {
            THROW_ERROR_EXCEPTION("Option 'build-snapshot' can only be used with 'validate-snapshot' or 'replay-changelog'");
        }

        if (ExportSnapshotFlag_ && !ExportConfigFlag_) {
            THROW_ERROR_EXCEPTION("Option 'export-snapshot' requires 'export-config' to be set");
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
            config->DryRun->EnableHostNameValidation = false;
            config->DryRun->EnableDryRun = true;
            auto loggingConfig = config->GetSingletonConfig<NLogging::TLogManagerConfig>();
            loggingConfig->ShutdownGraceTimeout = TDuration::Seconds(10);
            config->Snapshots->Path = NFS::GetDirectoryName(".");

            if (SkipTvmServiceEnvValidationFlag_) {
                auto authManagerConfig = config->GetSingletonConfig<NAuth::TNativeAuthenticationManagerConfig>();
                authManagerConfig->EnableValidation = false;
                authManagerConfig->EnableSubmission = false;
                authManagerConfig->TvmService = nullptr;
            }
        }

        if (IsDumpSnapshotMode()) {
            config->HydraManager->SnapshotBackgroundThreadCount = 0;
            config->SetSingletonConfig(NLogging::TLogManagerConfig::CreateSilent());
        }

        if (IsValidateSnapshotMode()) {
            config->SetSingletonConfig(NHydra::CreateDryRunLoggingConfig());
        }

        if (IsExportSnapshotMode()) {
            config->SetSingletonConfig(NLogging::TLogManagerConfig::CreateQuiet());
        }

        if (IsBuildSnapshotMode()) {
            config->Snapshots->Path = NFS::GetRealPath(BuildSnapshotPath_);
        }
    }

    void DoStart() final
    {
        if (IsPrintCompatibilityInfoMode()) {
            DoPrintCompatibilityInfo();
            return;
        }

        auto bootstrap = CreateMasterBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);

        if (IsDryRunMode()) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();

            bootstrap->Initialize();

            if (IsLoadSnapshotMode()) {
                bootstrap->LoadSnapshot(
                    LoadSnapshotPath_,
                    IsDumpSnapshotMode() ? SnapshotDumpMode_ : ESerializationDumpMode::None);
            }

            if (IsExportSnapshotMode()) {
                // TODO (h0pless): maybe rename this to ExportState
                ExportSnapshot(bootstrap.get(), ExportConfigPath_);
            }

            if (IsReplayChangelogsMode()) {
                bootstrap->ReplayChangelogs(ReplayChangelogsPaths_);
            }

            if (IsBuildSnapshotMode()) {
                bootstrap->BuildSnapshot();
            }

            bootstrap->FinishDryRun();

            // XXX(babenko): ASAN complains about memory leak on graceful exit.
            // Must try to resolve them later.
            AbortProcessSilently(EProcessExitCode::OK);
        }

        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }

    void DoPrintCompatibilityInfo()
    {
        NYson::TYsonWriter writer(&Cout, NYson::EYsonFormat::Pretty);
        auto info = NYTree::BuildYsonStringFluently()
            .BeginMap()
                .Item("current_reign").Value(NCellMaster::GetCurrentReign())
            .EndMap();
        NYson::Serialize(info, &writer);
        Cout << Endl;
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunCellMasterProgram(int argc, const char** argv)
{
    TCellMasterProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
