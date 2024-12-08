#include "program.h"

#include "bootstrap.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerProgram
    : public TServerProgram
    , public TProgramConfigMixin<TSchedulerBootstrapConfig>
{
public:
    TSchedulerProgram()
        : TProgramConfigMixin(Opts_)
    {
        SetMainThreadName("Scheduler");
    }

private:
    void DoStart() final
    {
        auto config = GetConfig();
        auto configNode = GetConfigNode();

        ConfigureNativeSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new TBootstrap(config, configNode);
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunSchedulerProgram(int argc, const char** argv)
{
    TSchedulerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
