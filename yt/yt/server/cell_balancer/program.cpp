#include "program.h"

#include "config.h"
#include "bootstrap.h"

#include <yt/yt/library/server_program/server_program.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerProgram
    : public TServerProgram<TCellBalancerProgramConfig>
{
public:
    TCellBalancerProgram()
    {
        SetMainThreadName("CBProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateCellBalancerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
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
