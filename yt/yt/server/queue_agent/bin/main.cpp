#include <yt/yt/server/queue_agent/program.h>

int main(int argc, const char** argv)
{
    return NYT::NQueueAgent::TQueueAgentProgram().Run(argc, argv);
}
