#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheProgram
    : public TServerProgram<TMasterCacheConfig>
{
public:
    TMasterCacheProgram()
    {
        SetMainThreadName("MasterCache");
    }

protected:
    void DoStart() final
    {
        auto* bootstrap = CreateBootstrap(GetConfig()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
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
