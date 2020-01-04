#include <yt/server/clock_server/cluster_clock/program.h>

int main(int argc, const char** argv)
{
    return NYT::TClusterClockProgram().Run(argc, argv);
}