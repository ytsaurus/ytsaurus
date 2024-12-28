#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerProgram
    : public TServerProgram<TTabletBalancerProgramConfig>
{
public:
    TTabletBalancerProgram()
    {
        SetMainThreadName("TabBalancerProg");
    }

private:
    void DoStart() final
    {
        auto bootstrap = CreateTabletBalancerBootstrap(GetConfig(), GetConfigNode(), GetServiceLocator());
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run()
            .Get()
            .ThrowOnError();
        SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunTabletBalancerProgram(int argc, const char** argv)
{
    TTabletBalancerProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
