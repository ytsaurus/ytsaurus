#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheProgram
    : public TServerProgram<TChaosCacheProgramConfig>
{
public:
    TChaosCacheProgram()
    {
        SetMainThreadName("ChaosCacheProg");
    }

protected:
    void DoStart() final
    {
        auto bootstrap = CreateChaosCacheBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunChaosCacheProgram(int argc, const char** argv)
{
    TChaosCacheProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
