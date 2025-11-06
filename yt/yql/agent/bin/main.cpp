#include <yt/yql/agent/program.h>

#include <yt/yql/plugin/process/public.h>
#include <yt/yql/plugin/process/program.h>

#include <yt/cpp/mapreduce/interface/client.h>

int main(int argc, const char** argv)
{
    if (TStringBuf(argv[1]) == NYT::NYqlPlugin::NProcess::YqlPluginSubcommandName) {
        NYT::NYqlPlugin::NProcess::RunYqlPluginProgram(argc - 1, argv + 1);
    } else {
        return NYT::NYqlAgent::TYqlAgentProgram().Run(argc, argv);
    }
}
