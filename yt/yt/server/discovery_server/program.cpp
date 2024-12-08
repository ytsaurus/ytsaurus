#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/helpers.h>

namespace NYT::NClusterDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

class TClusterDiscoveryServerProgram
    : public TServerProgram
    , public TProgramConfigMixin<NClusterDiscoveryServer::TClusterDiscoveryServerConfig>
{
public:
    TClusterDiscoveryServerProgram()
        : TProgramConfigMixin(Opts_)
    {
        SetMainThreadName("DiscoveryServer");
    }

protected:
    void DoStart() override
    {
        auto config = GetConfig();

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = NClusterDiscoveryServer::CreateBootstrap(std::move(config)).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunClusterDiscoveryServerProgram(int argc, const char** argv)
{
    TClusterDiscoveryServerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterDiscoveryServer
