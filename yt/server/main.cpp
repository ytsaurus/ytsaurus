#include "stdafx.h"

#include <ytlib/misc/enum.h>
#include <ytlib/misc/errortrace.h>
#include <ytlib/rpc/rpc_manager.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/chunk_holder/config.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>
#include <ytlib/cell_node/bootstrap.h>
#include <ytlib/cell_node/config.h>
#include <ytlib/cell_node/bootstrap.h>
#include <ytlib/cell_scheduler/config.h>
#include <ytlib/cell_scheduler/bootstrap.h>
#include <ytlib/scheduler/config.h>
#include <ytlib/job_proxy/config.h>
#include <ytlib/job_proxy/job_proxy.h>

namespace NYT {

using namespace NLastGetopt;
using namespace NYTree;
using namespace NElection;
using namespace NScheduler;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Server");

DECLARE_ENUM(EExitCode,
    ((OK)(0))
    ((OptionsError)(1))
    ((BootstrapError)(2))
);

EExitCode GuardedMain(int argc, const char* argv[])
{
    // Configure options parser.
    TOpts opts;

    opts.AddHelpOption();

    const auto& cellNodeOpt = opts.AddLongOption("node", "start cell node")
        .NoArgument()
        .Optional();

    const auto& cellMasterOpt = opts.AddLongOption("master", "start cell master")
        .NoArgument()
        .Optional();

    const auto& schedulerOpt = opts.AddLongOption("scheduler", "start scheduler")
        .NoArgument()
        .Optional();

    const TOpt& jobProxyOpt = opts.AddLongOption("job-proxy", "start job proxy")
        .NoArgument()
        .Optional();

    Stroka jobIdOpt;
    opts.AddLongOption("job-id", "job id (for job-proxy mode)")
        .Optional()
        .RequiredArgument("ID")
        .StoreResult(&jobIdOpt);

    int port = -1;
    opts.AddLongOption("port", "port to listen")
        .Optional()
        .RequiredArgument("PORT")
        .StoreResult(&port);

    Stroka configFileName;
    opts.AddLongOption("config", "configuration file")
        .Optional()
        .RequiredArgument("FILE")
        .StoreResult(&configFileName);

    const auto& configTemplateOpt = opts.AddLongOption("config-template", "print configuration file template")
        .NoArgument()
        .Optional();

    TOptsParseResult results(&opts, argc, argv);

    // Figure out the mode: cell master or chunk holder.
    bool isCellMaster = results.Has(&cellMasterOpt);
    bool isCellNode = results.Has(&cellNodeOpt);
    bool isScheduler = results.Has(&schedulerOpt);
    bool isJobProxy = results.Has(&jobProxyOpt);

    int modeCount = 0;
    if (isCellNode) {
        ++modeCount;
    }
    if (isCellMaster) {
        ++modeCount;
    }

    if (isScheduler) {
        ++modeCount;
    }

    if (isJobProxy) {
        ++modeCount;
    }

    if (modeCount != 1) {
        opts.PrintUsage(results.GetProgramName());
        return EExitCode::OptionsError;
    }

    INodePtr configNode;
    if (!results.Has(&configTemplateOpt)) {
        // Configure logging.
        NLog::TLogManager::Get()->Configure(configFileName, "logging");

        // Parse configuration file.
        try {
            TIFStream configStream(configFileName);
            configNode = DeserializeFromYson(&configStream);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error reading server configuration\n%s",
                ex.what());
        }
    }

    // Start an appropriate server.
    if (isCellNode) {
        auto config = New<NCellNode::TCellNodeConfig>();
        if (results.Has(&configTemplateOpt)) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(~configNode, false);

            // Override RPC port.
            // TODO(babenko): enable overriding arbitrary options from the command line
            if (port >= 0) {
                config->RpcPort = port;
            }

            config->Validate();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing cell node configuration\n%s",
                ex.what());
        }


        NCellNode::TBootstrap bootstrap(configFileName, config);
        bootstrap.Run();
    }

    if (isCellMaster) {
        auto config = New<NCellMaster::TCellMasterConfig>();
        if (results.Has(&configTemplateOpt)) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(~configNode, false);

            // Override RPC port.
            if (port >= 0) {
                config->MetaState->Cell->RpcPort = port;
            }

            config->Validate();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing cell master configuration\n%s",
                ex.what());
        }

        NCellMaster::TBootstrap bootstrap(configFileName, config);
        bootstrap.Run();
    }

    if (isScheduler) {
        auto config = New<NCellScheduler::TCellSchedulerConfig>();
        if (results.Has(&configTemplateOpt)) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(~configNode);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing cell scheduler configuration\n%s",
                ex.what());
        }

        NCellScheduler::TBootstrap bootstrap(configFileName, config);
        bootstrap.Run();
    }

    if (isJobProxy) {
        auto config = New<NJobProxy::TJobProxyConfig>();
        if (results.Has(&configTemplateOpt)) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        TJobId jobId;
        try {
            jobId = TGuid::FromString(jobIdOpt);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Invalid job-id value: %s",
                ex.what());
            return EExitCode::OptionsError;
        }

        try {
            config->Load(~configNode);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing job-proxy configuration\n%s",
                ex.what());
        }

        TJobProxy jobProxy(~config, jobId);
        jobProxy.Run();
    }

    // Actually this will never happen.
    return EExitCode::OK;
}

int Main(int argc, const char* argv[])
{
    NYT::SetupErrorHandler();
    NProfiling::TProfilingManager::Get()->Start();

    int exitCode;
    try {
        exitCode = GuardedMain(argc, argv);
    }
    catch (const std::exception& ex) {
        LOG_ERROR("Server startup failed\n%s", ex.what());
        exitCode = EExitCode::BootstrapError;
    }

    // TODO: refactor system shutdown
    NLog::TLogManager::Get()->Shutdown();
    NProfiling::TProfilingManager::Get()->Shutdown();
    NRpc::TRpcManager::Get()->Shutdown();
    TDelayedInvoker::Shutdown();

    return exitCode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    return NYT::Main(argc, argv);
}
