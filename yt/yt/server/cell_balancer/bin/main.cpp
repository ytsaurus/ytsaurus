#include <yt/yt/server/cell_balancer/program.h>

int main(int argc, const char** argv)
{
    return NYT::NCellBalancer::TCellBalancerProgram().Run(argc, argv);
}
