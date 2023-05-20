#include "bootstrap.h"
#include "config.h"
#include "snapshot_exporter.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/file_log_writer.h>
#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/misc/shutdown.h>

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
            .StoreMappedResult(&DumpSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("dump-config", "config for snapshot dumping, which contains 'lower_limit' and 'upper_limit'")
            .StoreResult(&DumpConfig_)
            .RequiredArgument("CONFIG_YSON");
        Opts_
            .AddLongOption("export-snapshot", "export master snapshot\nexpects path to snapshot")
            .StoreMappedResult(&ExportSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("export-config", "user config for master snapshot exporting\nexpects yson which may have keys "
                           "'attributes', 'first_key', 'last_key', 'types', 'job_index', 'job_count'")
            .StoreResult(&ExportSnapshotConfig_)
            .RequiredArgument("CONFIG_YSON");
        Opts_
            .AddLongOption("validate-snapshot", "load master snapshot in a dry run mode")
            .StoreMappedResult(&ValidateSnapshot_, &CheckPathExistsArgMapper)
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
            .AddLongOption("force-log-level-info", "set log level to info for dry run")
            .SetFlag(&ForceLogLevelInfo_)
            .NoArgument();
        Opts_
            .AddLongOption("report-total-write-count")
            .SetFlag(&EnableTotalWriteCountReport_)
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

        auto isDryRun = dumpSnapshot || validateSnapshot || exportSnapshot || replayChangelogs;

        if (isDryRun) {
            NBus::TTcpDispatcher::Get()->DisableNetworking();
            config->DryRun->EnableHostNameValidation = false;
            config->DryRun->EnableDryRun = true;
            config->Logging->ShutdownGraceTimeout = TDuration::Seconds(10);

            if (ForceLogLevelInfo_) {
                config->Logging->Rules[0]->MinLevel = NLogging::ELogLevel::Info;
            }
        }

        if (replayChangelogs) {
            auto changelogDirectory = NFS::GetDirectoryName(ChangelogFileNames_.front());
            for (const auto& fileName : ChangelogFileNames_) {
                if (changelogDirectory != NFS::GetDirectoryName(fileName)) {
                    THROW_ERROR_EXCEPTION("Changelogs must be located in one directory");
                }
            }
        }

        if (buildSnapshot) {
            config->Snapshots->Path = SnapshotBuildDirectory_.empty()
                ? NFS::GetDirectoryName(".")
                : NFS::GetRealPath(SnapshotBuildDirectory_);
        }

        if (dumpSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateSilent();
        } else if (validateSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();

            auto silentRule = New<NLogging::TRuleConfig>();
            silentRule->MinLevel = NLogging::ELogLevel::Debug;
            silentRule->Writers.push_back(TString("dev_null"));

            auto writerConfig = New<NLogging::TLogWriterConfig>();
            writerConfig->Type = NLogging::TFileLogWriterConfig::Type;

            auto fileWriterConfig = New<NLogging::TFileLogWriterConfig>();
            fileWriterConfig->FileName = "/dev/null";

            config->Logging->Rules.push_back(silentRule);
            config->Logging->Writers.emplace(TString("dev_null"), writerConfig->BuildFullConfig(fileWriterConfig));
        } else if (exportSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();
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

        if (dumpSnapshot) {
            bootstrap->LoadSnapshotOrThrow(DumpSnapshot_, true, false, DumpConfig_);
        }

        if (exportSnapshot) {
            ExportSnapshot(bootstrap, ExportSnapshot_, ExportSnapshotConfig_);
        }

        if (validateSnapshot) {
            bootstrap->LoadSnapshotOrThrow(ValidateSnapshot_, false, EnableTotalWriteCountReport_, {});
        }
        if (replayChangelogs) {
            bootstrap->ReplayChangelogsOrThrow(std::move(ChangelogFileNames_));
        }
        if (buildSnapshot) {
            bootstrap->BuildSnapshotOrThrow();
        }

        if (isDryRun) {
            bootstrap->FinishDryRunOrThrow();
        } else {
            bootstrap->Run();
        }

        // XXX(babenko): ASAN complains about memory leak on graceful exit.
        // Must try to resolve them later.
        _exit(0);
    }

private:
    TString DumpSnapshot_;
    TString DumpConfig_;
    TString ExportSnapshot_;
    TString ExportSnapshotConfig_;
    TString ValidateSnapshot_;
    std::vector<TString> ChangelogFileNames_;
    TString SnapshotBuildDirectory_;
    bool EnableTotalWriteCountReport_ = false;
    bool SleepAfterInitialize_ = false;
    bool ForceLogLevelInfo_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
