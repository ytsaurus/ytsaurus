#include "bootstrap.h"
#include "config.h"
#include "snapshot_exporter.h"

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/config.h>

#include <yt/library/phdr_cache/phdr_cache.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/core/ytalloc/bindings.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellMasterProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NCellMaster::TCellMasterConfig>
{
public:
    TCellMasterProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_
            .AddLongOption("dump-snapshot", "dump master snapshot and exit")
            .StoreMappedResult(&DumpSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("validate-snapshot", "validate master snapshot and exit")
            .StoreMappedResult(&ValidateSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("export-snapshot", "export master snapshot\nexpects path to snapshot")
            .StoreMappedResult(&ExportSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("export-config", "user config for master snapshot exporting\nexpects yson which may have keys 'attributes', 'first_key', 'last_key', 'types'")
            .StoreResult(&ExportSnapshotConfig_)
            .RequiredArgument("CONFIG_YSON");
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("MasterMain");

        bool dumpSnapshot = parseResult.Has("dump-snapshot");
        bool validateSnapshot = parseResult.Has("validate-snapshot");
        bool exportSnapshot = parseResult.Has("export-snapshot");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        EnableRefCountedTrackerProfiling();
        NYTAlloc::EnableYTLogging();
        NYTAlloc::EnableYTProfiling();
        NYTAlloc::SetLibunwindBacktraceProvider();
        NYTAlloc::ConfigureFromEnv();
        NYTAlloc::EnableStockpile();
        NYTAlloc::MlockallCurrentProcess();

        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        if (dumpSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateSilent();
        } else if (validateSnapshot || exportSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();
        }

        ConfigureSingletons(config);

        if (dumpSnapshot || validateSnapshot || exportSnapshot) {
            NLogging::TLogManager::Get()->ConfigureFromEnv();
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellMaster::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Initialize();

        if (dumpSnapshot) {
            bootstrap->TryLoadSnapshot(DumpSnapshot_, true);
        } else if (validateSnapshot) {
            bootstrap->TryLoadSnapshot(ValidateSnapshot_, false);
        } else if (exportSnapshot) {
            ExportSnapshot(bootstrap, ExportSnapshot_, ExportSnapshotConfig_);
        } else {
            bootstrap->Run();
        }
    }

private:
    TString DumpSnapshot_;
    TString ValidateSnapshot_;
    TString ExportSnapshot_;
    TString ExportSnapshotConfig_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
