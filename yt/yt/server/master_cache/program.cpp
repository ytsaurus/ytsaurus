#include "program.h"

#include <yt/ytlib/program/helpers.h>

#include <yt/library/phdr_cache/phdr_cache.h>

#include <yt/core/bus/tcp/dispatcher.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/core/ytalloc/bindings.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

TMasterCacheProgram::TMasterCacheProgram()
    : TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin(Opts_)
{ }

void TMasterCacheProgram::DoRun(const NLastGetopt::TOptsParseResult& parseResult)
{
    TThread::SetCurrentThreadName("MasterCacheMain");

    ConfigureUids();
    ConfigureIgnoreSigpipe();
    ConfigureCrashHandler();
    ConfigureExitZeroOnSigterm();
    EnablePhdrCache();
    EnableRefCountedTrackerProfiling();
    NYTAlloc::EnableYTLogging();
    NYTAlloc::EnableYTProfiling();
    NYTAlloc::InitializeLibunwindInterop();
    NYTAlloc::SetEnableEagerMemoryRelease(false);
    NYTAlloc::EnableStockpile();
    NYTAlloc::MlockFileMappings();

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

    ConfigureSingletons(config);
    StartDiagnosticDump(config);

    // TODO(babenko): This memory leak is intentional.
    // We should avoid destroying bootstrap since some of the subsystems
    // may be holding a reference to it and continue running some actions in background threads.
    auto* bootstrap = CreateBootstrap(std::move(config)).release();
    bootstrap->Initialize();
    bootstrap->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
