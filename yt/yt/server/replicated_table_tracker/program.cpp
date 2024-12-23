#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableTrackerProgram
    : public TServerProgram<TReplicatedTableTrackerServerConfig>
{
public:
    TReplicatedTableTrackerProgram()
    {
        SetMainThreadName("Rtt");
    }

    void DoStart() final
    {
        auto* bootstrap = CreateBootstrap(GetConfig(), GetConfigNode()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
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
