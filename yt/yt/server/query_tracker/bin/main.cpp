#include <yt/yt/server/query_tracker/program.h>

int main(int argc, const char** argv)
{
    return NYT::NQueryTracker::TQueryTrackerProgram().Run(argc, argv);
}
