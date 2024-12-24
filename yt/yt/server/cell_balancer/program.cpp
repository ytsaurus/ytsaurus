#include "program.h"

#include "config.h"
#include "bootstrap.h"

#include <yt/yt/library/server_program/server_program.h>

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
        auto* bootstrap = CreateBootstrap(GetConfig()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Initialize();
        bootstrap->Run();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunCellBalancerProgram(int argc, const char** argv)
{
    TCellBalancerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
