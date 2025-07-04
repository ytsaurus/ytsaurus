#include <yt/yql/agent/program.h>

#include <yt/yql/plugin/process/program.h>

#include <yt/cpp/mapreduce/interface/client.h>

int main(int argc, const char** argv)
{
    if (TStringBuf(argv[1]).StartsWith(NYT::NYqlPlugin::NProcess::YqlPluginSubcommandName)) {
        return NYT::NYqlPlugin::NProcess::TYqlPluginProgram().Run(argc - 1, argv + 1);
    }

    return NYT::NYqlAgent::TYqlAgentProgram().Run(argc, argv);
}
