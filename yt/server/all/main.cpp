#include <yt/server/master/cell_master/program.h>
#include <yt/server/http_proxy/program.h>
#include <yt/server/rpc_proxy/program.h>
#include <yt/server/job_proxy/program.h>
#include <yt/server/scheduler/program.h>
#include <yt/server/controller_agent/program.h>
#include <yt/server/tools/program.h>
#include <yt/server/node/cell_node/program.h>
#include <yt/server/exec/program.h>

int main(int argc, const char** argv)
{
    std::vector<std::pair<TString, std::function<int()>>> programs = {
        {"ytserver-master", [&] { return NYT::TCellMasterProgram().Run(argc, argv); }},
        {"ytserver-http-proxy", [&] { return NYT::THttpProxyProgram().Run(argc, argv); }},
        {"ytserver-proxy", [&] { return NYT::NCellProxy::TCellProxyProgram().Run(argc, argv); }},
        {"ytserver-node", [&] { return NYT::NCellNode::TCellNodeProgram().Run(argc, argv); }},
        {"ytserver-job-proxy", [&] { return NYT::NJobProxy::TJobProxyProgram().Run(argc, argv); }},
        {"ytserver-exec", [&] { return NYT::NJobProxy::TExecProgram().Run(argc, argv); }},
        {"ytserver-tools", [&] { return NYT::TToolsProgram().Run(argc, argv); }},
        {"ytserver-scheduler", [&] { return NYT::NScheduler::TSchedulerProgram().Run(argc, argv); }},
        {"ytserver-controller-agent", [&] { return NYT::NControllerAgent::TControllerAgentProgram().Run(argc, argv); }},
    };

    for (const auto program : programs) {
        if (TStringBuf(argv[0]).EndsWith(program.first)) {
            return program.second();
        }
    }

    Cerr << "Program " << argv[0] << " is not known" << Endl;
    return 1;
}
