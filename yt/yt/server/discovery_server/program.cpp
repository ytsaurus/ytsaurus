#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterDiscoveryServerProgram
    : public TServerProgram<TDiscoveryServerProgramConfig>
{
public:
    TClusterDiscoveryServerProgram()
    {
        SetMainThreadName("DiscServerProg");
    }

protected:
    void DoStart() override
    {
        auto bootstrap = CreateDiscoveryServerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunClusterDiscoveryServerProgram(int argc, const char** argv)
{
    TClusterDiscoveryServerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
