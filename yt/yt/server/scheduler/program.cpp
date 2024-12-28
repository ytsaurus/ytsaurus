#include "program.h"

#include "bootstrap.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerProgram
    : public TServerProgram<TSchedulerProgramConfig>
{
public:
    TSchedulerProgram()
    {
        SetMainThreadName("SchedulerProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateSchedulerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
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
