#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterDiscoveryServerProgram
    : public TServerProgram<TClusterDiscoveryServerConfig>
{
public:
    TClusterDiscoveryServerProgram()
    {
        SetMainThreadName("DiscoveryServer");
    }

protected:
    void DoStart() override
    {
        auto* bootstrap = NClusterDiscoveryServer::CreateBootstrap(GetConfig()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
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
