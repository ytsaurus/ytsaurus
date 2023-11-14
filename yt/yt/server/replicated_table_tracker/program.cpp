#include "bootstrap.h"
#include "program.h"

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <util/system/thread.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableTrackerProgram::TReplicatedTableTrackerProgram()
    : TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin(Opts_)
{ }

void TReplicatedTableTrackerProgram::DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/)
{
    TThread::SetCurrentThreadName("RttMain");

    ConfigureUids();
    ConfigureIgnoreSigpipe();
    ConfigureCrashHandler();
    ConfigureExitZeroOnSigterm();
    EnablePhdrCache();
    ConfigureAllocator();

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

    ConfigureNativeSingletons(config);
    StartDiagnosticDump(config);

    auto configNode = GetConfigNode();

    auto* bootstrap = CreateBootstrap(std::move(config), std::move(configNode)).release();
    bootstrap->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
