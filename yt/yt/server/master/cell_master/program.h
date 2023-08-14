#include "bootstrap.h"
#include "config.h"
#include "snapshot_exporter.h"

#include <yt/yt/server/lib/hydra_common/dry_run/utils.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/file_log_writer.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NCellMaster::TCellMasterConfig>
{
public:
    TCellMasterProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_
            .AddLongOption("dump-snapshot", "dump master snapshot and exit")
            .StoreMappedResult(&SnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("dump-config", "config for snapshot dumping, which contains 'lower_limit' and 'upper_limit'")
            .StoreResult(&DumpSnapshotConfig_)
            .RequiredArgument("CONFIG_YSON");
        Opts_
            .AddLongOption("export-snapshot", "export master snapshot\nexpects path to snapshot")
            .StoreMappedResult(&SnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("export-config", "user config for master snapshot exporting\nexpects yson which may have keys "
                           "'attributes', 'first_key', 'last_key', 'types', 'job_index', 'job_count'")
            .StoreResult(&ExportSnapshotConfig_)
            .RequiredArgument("CONFIG_YSON");
        Opts_
            .AddLongOption("validate-snapshot", "load master snapshot in a dry run mode")
            .StoreMappedResult(&SnapshotPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("replay-changelogs", "replay one or more consecutive master changelogs\n"
                           "Usually used in conjunction with 'validate-snapshot' option to apply changelogs over a specific snapshot")
            .SplitHandler(&ChangelogFileNames_, ' ')
            .RequiredArgument("CHANGELOG");
        Opts_
            .AddLongOption("build-snapshot", "save resulting state in a snapshot\n"
                                             "Path to a directory can be specified here\n"
                                             "By default snapshot will be saved in the working directory")
            .StoreResult(&SnapshotBuildDirectory_)
            .OptionalArgument("DIRECTORY");
        Opts_
            .AddLongOption("report-total-write-count")
            .SetFlag(&EnableTotalWriteCountReport_)
            .NoArgument();
        Opts_
            .AddLongOption("skip-tvm-service-env-validation", "don't validate tvm service files")
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
        TThread::SetCurrentThreadName("MasterMain");

        auto dumpSnapshot = parseResult.Has("dump-snapshot");
        auto exportSnapshot = parseResult.Has("export-snapshot");
        auto validateSnapshot = parseResult.Has("validate-snapshot");
        auto replayChangelogs = parseResult.Has("replay-changelogs");
        auto buildSnapshot = parseResult.Has("build-snapshot");

        if (dumpSnapshot + validateSnapshot + exportSnapshot > 1) {
            THROW_ERROR_EXCEPTION("Options 'dump-snapshot', 'validate-snapshot' and 'export-snapshot' are mutually exclusive");
        }

        if ((dumpSnapshot || exportSnapshot) && replayChangelogs) {
            THROW_ERROR_EXCEPTION("Option 'replay-changelogs' can not be used with 'dump-snapshot' or 'export-snapshot'");
        }

        if (buildSnapshot && !replayChangelogs && !validateSnapshot) {
            THROW_ERROR_EXCEPTION("Option 'build-snapshot' can only be used with 'validate-snapshot' or 'replay-changelog'");
        }

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        ConfigureAllocator();
        MlockFileMappings();

        if (HandleSetsidOptions()) {
            return;
        }
        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();

        auto loadSnapshot = dumpSnapshot || validateSnapshot || exportSnapshot;
        auto isDryRun = loadSnapshot || replayChangelogs;

        if (isDryRun) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();
            config->DryRun->EnableHostNameValidation = false;
            config->DryRun->EnableDryRun = true;
            config->Logging->ShutdownGraceTimeout = TDuration::Seconds(10);
            config->Snapshots->Path = NFS::GetDirectoryName(".");

            if (SkipTvmServiceEnvValidation_) {
                const auto& nativeAuthenticationManager = config->NativeAuthenticationManager;
                nativeAuthenticationManager->EnableValidation = false;
                nativeAuthenticationManager->EnableSubmission = false;
                nativeAuthenticationManager->TvmService = nullptr;
            }
        }

        if (replayChangelogs) {
            auto changelogDirectory = NFS::GetDirectoryName(ChangelogFileNames_.front());
            for (const auto& fileName : ChangelogFileNames_) {
                THROW_ERROR_EXCEPTION_IF(
                    changelogDirectory != NFS::GetDirectoryName(fileName),
                    "Changelogs must be located in one directory");
            }
        }

        if (dumpSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateSilent();
        } else if (validateSnapshot) {
            NHydra::ConfigureDryRunLogging(config);
        } else if (exportSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();
        }

        if (buildSnapshot && !SnapshotBuildDirectory_.empty()) {
            config->Snapshots->Path = NFS::GetRealPath(SnapshotBuildDirectory_);
        }

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellMaster::TBootstrap(std::move(config));
        bootstrap->Initialize();

        if (SleepAfterInitialize_) {
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(10));
        }

        if (!isDryRun) {
            bootstrap->Run();
        } else {
            if (loadSnapshot) {
                bootstrap->LoadSnapshotOrThrow(SnapshotPath_, dumpSnapshot, EnableTotalWriteCountReport_, DumpSnapshotConfig_);
            }
            if (exportSnapshot) {
                // TODO (h0pless): maybe rename this to ExportState
                ExportSnapshot(bootstrap, ExportSnapshotConfig_);
            }
            if (replayChangelogs) {
                bootstrap->ReplayChangelogsOrThrow(std::move(ChangelogFileNames_));
            }
            if (buildSnapshot) {
                bootstrap->BuildSnapshotOrThrow();
            }
            bootstrap->FinishDryRunOrThrow();
        }

        // XXX(babenko): ASAN complains about memory leak on graceful exit.
        // Must try to resolve them later.
        _exit(0);
    }

private:
    TString SnapshotPath_;
    TString DumpSnapshotConfig_;
    TString ExportSnapshotConfig_;
    std::vector<TString> ChangelogFileNames_;
    TString SnapshotBuildDirectory_;
    bool EnableTotalWriteCountReport_ = false;
    bool SkipTvmServiceEnvValidation_ = false;
    bool SleepAfterInitialize_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
