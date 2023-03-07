#include <yt/server/controller_agent/program.h>

int main(int argc, const char** argv)
{
    return NYT::NControllerAgent::TControllerAgentProgram().Run(argc, argv);
}
