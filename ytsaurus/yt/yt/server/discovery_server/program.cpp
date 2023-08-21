#include "program.h"

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

TClusterDiscoveryServerProgram::TClusterDiscoveryServerProgram()
    : TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin(Opts_)
{ }

void TClusterDiscoveryServerProgram::DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/)
{
    TThread::SetCurrentThreadName("DiscoveryMain");

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

    ConfigureSingletons(config);
    StartDiagnosticDump(config);

    // TODO(babenko): This memory leak is intentional.
    // We should avoid destroying bootstrap since some of the subsystems
    // may be holding a reference to it and continue running some actions in background threads.
    auto* bootstrap = NClusterDiscoveryServer::CreateBootstrap(std::move(config)).release();
    bootstrap->Initialize();
    bootstrap->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
