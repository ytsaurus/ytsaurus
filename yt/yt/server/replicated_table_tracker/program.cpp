#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerProgram
    : public TServerProgram<TReplicatedTableTrackerProgramConfig>
{
public:
    TReplicatedTableTrackerProgram()
    {
        SetMainThreadName("RttProg");
    }

    void DoStart() final
    {
        auto bootstrap = CreateReplicatedTableTrackerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunReplicatedTableTrackerProgram(int argc, const char** argv)
{
    TReplicatedTableTrackerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
