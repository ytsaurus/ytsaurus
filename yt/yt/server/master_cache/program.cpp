#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheProgram
    : public TServerProgram<TMasterCacheProgramConfig>
{
public:
    TMasterCacheProgram()
    {
        SetMainThreadName("MasterCacheProg");
    }

protected:
    void DoStart() final
    {
        auto bootstrap = CreateMasterCacheBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunMasterCacheProgram(int argc, const char** argv)
{
    TMasterCacheProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
