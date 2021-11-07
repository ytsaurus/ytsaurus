#include <yt/yt/server/clock_server/cluster_clock/program.h>

int main(int argc, const char** argv)
{
    return NYT::NClusterClock::TClusterClockProgram().Run(argc, argv);
}
