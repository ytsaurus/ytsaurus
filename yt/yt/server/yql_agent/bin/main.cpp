#include <yt/yt/server/yql_agent/program.h>

#include <mapreduce/yt/interface/client.h>

int main(int argc, const char** argv)
{
    NYT::Initialize(argc, argv);

    return NYT::NYqlAgent::TYqlAgentProgram().Run(argc, argv);
}
