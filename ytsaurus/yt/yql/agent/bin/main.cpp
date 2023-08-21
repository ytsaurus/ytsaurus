#include <yt/yql/agent/program.h>

#include <yt/cpp/mapreduce/interface/client.h>

int main(int argc, const char** argv)
{
    return NYT::NYqlAgent::TYqlAgentProgram().Run(argc, argv);
}
