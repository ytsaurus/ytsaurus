#include <yt/yt/server/replicated_table_tracker/program.h>

int main(int argc, const char** argv)
{
    return NYT::NReplicatedTableTracker::TReplicatedTableTrackerProgram().Run(argc, argv);
}
