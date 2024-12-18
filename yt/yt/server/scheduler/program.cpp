#include "program.h"

#include "bootstrap.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerProgram
    : public TServerProgram<TSchedulerBootstrapConfig>
{
public:
    TSchedulerProgram()
    {
        SetMainThreadName("Scheduler");
    }

private:
    void DoStart() final
    {
        auto* bootstrap = new TBootstrap(GetConfig(), GetConfigNode());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunSchedulerProgram(int argc, const char** argv)
{
    TSchedulerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
