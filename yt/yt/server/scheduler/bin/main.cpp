#include <yt/server/scheduler/program.h>

int main(int argc, const char** argv)
{
    return NYT::NScheduler::TSchedulerProgram().Run(argc, argv);
}
