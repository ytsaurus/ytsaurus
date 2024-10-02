#ifndef PROGRAM_INL_H_
#error "Direct inclusion of this file is not allowed, include program.h"
// For the sake of sane code completion.
#include "program.h"
#endif

#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <yt/yt/build/build.h>
#include <yt/yt/build/ya_version.h>

#include <yt/yt/library/containers/porto_resource_tracker.h>
#include <yt/yt/library/program/helpers.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NOrm::NServer::NProgram {

////////////////////////////////////////////////////////////////////////////////

template <class TDerivedProgram, class TMasterConfig>
TMasterProgram<TDerivedProgram, TMasterConfig>::TMasterProgram(int dbVersion)
    : TProgramPdeathsigMixin(Opts_)
    , TProgramConfigMixin<TMasterConfig>(Opts_)
    , NMaster::TDBVersionGetter(Opts_, dbVersion)
    , DBVersion_(dbVersion)
{ }

template <class TDerivedProgram, class TMasterConfig>
void TMasterProgram<TDerivedProgram, TMasterConfig>::PrintVersionAndExit()
{
    Cout << "db-" << DBVersion_ << "-" << CreateBranchCommitVersion(GetBranch()) << Endl;
    Exit(0);
}

template <class TDerivedProgram, class TMasterConfig>
void TMasterProgram<TDerivedProgram, TMasterConfig>::DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/)
{
    // Do not set thread name (previously was MasterMain) to allow users to use common `killall <binary>` instead of `killall MasterMain`.

    ConfigureIgnoreSigpipe();
    ConfigureCrashHandler();
    EnablePhdrCache();
    ConfigureExitZeroOnSigterm();
    ConfigureAllocator();

    if (HandlePdeathsigOptions()) {
        return;
    }

    if (TProgramConfigMixin<TMasterConfig>::HandleConfigOptions()) {
        return;
    }

    if (HandleGetDBVersion()) {
        return;
    }

    auto config = TProgramConfigMixin<TMasterConfig>::GetConfig();

    ConfigureSingletons(config);
    StartDiagnosticDump(config);

    if (config->EnablePortoResourceTracker) {
        NContainers::EnablePortoResourceTracker(config->PodSpec);
    }

    TDerivedProgram::StartBootstrap(
        std::move(config),
        TProgramConfigMixin<TMasterConfig>::GetConfigNode());
    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NProgram
