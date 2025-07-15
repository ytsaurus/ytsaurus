#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/profiling/perf/event_counter_profiler.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentProgram
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TYqlAgentServerConfig>
{
public:
    TYqlAgentProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    void DoRun() override
    {
        TThread::SetCurrentThreadName("Main");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        // We intentionally omit EnablePhdrCache() because not only YQL is loaded as a shared library, but also
        // YQL UDFs, and they may be user-provided in runtime for particular query.
        ConfigureAllocator({});
        MlockFileMappings();
        RunMixinCallbacks();

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        ConfigureSingletons(config);

        NProfiling::EnablePerfEventCounterProfiling();

        if (config->EnablePortoResourceTracker) {
            NContainers::EnablePortoResourceTracker(config->PodSpec);
        }

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
