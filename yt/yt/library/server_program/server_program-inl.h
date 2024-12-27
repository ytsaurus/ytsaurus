#ifndef SERVER_PROGRAM_INL_H_
#error "Direct inclusion of this file is not allowed, include server_program.h"
// For the sake of sane code completion
#include "server_program.h"
#endif

#include "config.h"

#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig>
TServerProgram<TConfig, TDynamicConfig>::TServerProgram()
    : TProgram()
    , TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin<TConfig, TDynamicConfig>(Opts_)
{ }

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::SetMainThreadName(const std::string& name)
{
    MainThreadName_ = name;
}

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::ValidateOpts()
{ }

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::TweakConfig()
{ }

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::SleepForever()
{
    Sleep(TDuration::Max());
    YT_ABORT();
}

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::DoRun()
{
    TThread::SetCurrentThreadName(MainThreadName_.c_str());

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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
