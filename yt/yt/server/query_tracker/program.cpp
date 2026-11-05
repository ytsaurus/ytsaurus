#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/build/build.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerProgram
    : public TServerProgram<TQueryTrackerProgramConfig>
{
public:
    TQueryTrackerProgram()
    {
        SetMainThreadName("QTProg");
    }

    //! Override to print Query Tracker component version instead of the global YT version.
    void PrintVersionAndExit() override
    {
        Cout << GetQueryTrackerVersion() << Endl;
        Exit(0);
    }

protected:
    void DoStart() final
    {
        auto bootstrap = CreateQueryTrackerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .BlockingGet()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunQueryTrackerProgram(int argc, const char** argv)
{
    TQueryTrackerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
