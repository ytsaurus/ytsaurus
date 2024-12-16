#include "program.h"

#include "config.h"
#include "bootstrap.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/ytlib/program/native_singletons.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerProgram
    : public TServerProgram<TCellBalancerBootstrapConfig>
{
public:
    TCellBalancerProgram()
    {
        SetMainThreadName("CellBalancer");
    }

private:
    void DoStart() final
    {
        auto config = GetConfig();

        ConfigureNativeSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = CreateBootstrap(std::move(config)).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunCellBalancerProgram(int argc, const char** argv)
{
    TCellBalancerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
