#include "stdafx.h"
#include "scheduler_bootstrap.h"

#include <ytlib/misc/enum.h>
#include <ytlib/misc/errortrace.h>
#include <ytlib/rpc/rpc_manager.h>
#include <ytlib/logging/log_manager.h>
#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>
#include <ytlib/cell_node/bootstrap.h>
#include <ytlib/cell_node/config.h>
#include <ytlib/cell_node/bootstrap.h>
#include <ytlib/chunk_holder/config.h>

namespace NYT {

using namespace NLastGetopt;
using namespace NYTree;
using namespace NElection;

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

    int port = -1;
    opts.AddLongOption("port", "port to listen")
        .Optional()
        .RequiredArgument("PORT")
        .StoreResult(&port);

    Stroka configFileName;
    opts.AddLongOption("config", "configuration file")
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

    if (modeCount != 1) {
        opts.PrintUsage(results.GetProgramName());
        return EExitCode::OptionsError;
    }

    // Configure logging.
    NLog::TLogManager::Get()->Configure(configFileName, "logging");

    // Parse configuration file.
    INodePtr configNode;
    try {
        TIFStream configStream(configFileName);
        configNode = DeserializeFromYson(&configStream);
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error reading server configuration\n%s",
            ex.what());
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
            if (port >= 0) {
                config->RpcPort = port;
            }

            config->Validate();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing chunk holder configuration\n%s",
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
                config->RpcPort = port;
            }

            config->Validate();
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing cell master configuration\n%s",
                ex.what());
        }

        NCellMaster::TBootstrap bootstrap(configFileName, ~config);
        bootstrap.Run();
    }

    if (isScheduler) {
        auto config = New<TSchedulerBootstrap::TConfig>();
        if (results.Has(&configTemplateOpt)) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(~configNode);
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing cell master configuration\n%s",
                ex.what());
        }

        TSchedulerBootstrap schedulerBootstrap(configFileName, ~config);
        schedulerBootstrap.Run();
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
