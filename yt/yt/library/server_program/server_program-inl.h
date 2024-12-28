#ifndef SERVER_PROGRAM_INL_H_
#error "Direct inclusion of this file is not allowed, include server_program.h"
// For the sake of sane code completion
#include "server_program.h"
#endif

#include "config.h"

#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/fusion/service_directory.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig>
TServerProgram<TConfig, TDynamicConfig>::TServerProgram()
    : TServerProgramBase()
    , TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin<TConfig, TDynamicConfig>(Opts_)
{ }

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::DoRun()
{
    TThread::SetCurrentThreadName(GetMainThreadName().c_str());

    RunMixinCallbacks();

    ValidateOpts();

    TweakConfig();

    Configure();

    DoStart();
}

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::Configure()
{
    ConfigureUids();

    ConfigureIgnoreSigpipe();

    ConfigureCrashHandler();

    ConfigureExitZeroOnSigterm();

    EnablePhdrCache();

    ConfigureAllocator();

    MlockFileMappings();

    auto config = this->GetConfig();

    ConfigureSingletons(config);

    if (config->EnablePortoResourceTracker) {
        NContainers::EnablePortoResourceTracker(config->PodSpec);
    }

    if (config->EnableRefCountedTrackerProfiling) {
        EnableRefCountedTrackerProfiling();
    }

    auto serviceDirectory = GetServiceDirectory();

    if (config->CoreDumper) {
        serviceDirectory->RegisterService(NCoreDump::CreateCoreDumper(config->CoreDumper));
    }

    auto solomonExporter = New<NProfiling::TSolomonExporter>(config->SolomonExporter);
    solomonExporter->Start();
    serviceDirectory->RegisterService(solomonExporter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
