#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/program/native_singletons.h>

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

class TQueryTrackerProgram
    : public TServerProgram<TQueryTrackerServerConfig>
{
public:
    TQueryTrackerProgram()
    {
        SetMainThreadName("QueryTracker");
    }

protected:
    void DoStart() final
    {
        auto* bootstrap = new TBootstrap(GetConfig(), GetConfigNode());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
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
