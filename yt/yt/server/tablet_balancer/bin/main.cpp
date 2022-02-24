#include <yt/yt/server/tablet_balancer/program.h>

int main(int argc, const char** argv)
{
    return NYT::NTabletBalancer::TTabletBalancerProgram().Run(argc, argv);
}
