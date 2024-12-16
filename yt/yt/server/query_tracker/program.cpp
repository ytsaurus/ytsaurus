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
        auto config = GetConfig();
        auto configNode = GetConfigNode();

        ConfigureNativeSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new TBootstrap(std::move(config), std::move(configNode));
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunQueryTrackerProgram(int argc, const char** argv)
{
    TQueryTrackerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
