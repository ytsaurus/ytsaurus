#include "program.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/library/program/helpers.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancerProgram
    : public TServerProgram<TTabletBalancerServerConfig>
{
public:
    TTabletBalancerProgram()
    {
        SetMainThreadName("TabletBalancer");
    }

private:
    void DoStart() final
    {
        auto* bootstrap = CreateBootstrap(GetConfig(), GetConfigNode()).release();
        DoNotOptimizeAway(bootstrap);
        bootstrap->Run();
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
