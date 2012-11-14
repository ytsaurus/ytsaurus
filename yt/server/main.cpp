#include "stdafx.h"

#include <ytlib/misc/crash_handler.h>
#include <ytlib/misc/tclap_helpers.h>

#include <ytlib/bus/tcp_dispatcher.h>

#include <ytlib/rpc/dispatcher.h>

#include <ytlib/logging/log_manager.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/meta_state/async_change_log.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <tclap/CmdLine.h>

#include <server/chunk_holder/config.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>
#include <server/cell_node/bootstrap.h>

#include <server/cell_scheduler/config.h>
#include <server/cell_scheduler/bootstrap.h>

#include <server/job_proxy/config.h>
#include <server/job_proxy/job_proxy.h>

#include <yt/build.h>

#include <util/system/sigset.h>

namespace NYT {

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

struct TArgsParser
{
public:
    TArgsParser()
        : CmdLine("Command line", ' ', YT_VERSION)
        , CellNode("", "node", "start cell node")
        , CellMaster("", "master", "start cell master")
        , Scheduler("", "scheduler", "start scheduler")
        , JobProxy("", "job-proxy", "start job proxy")
        , JobId("", "job-id", "job id (for job proxy mode)", false, "", "ID")
        , Port("", "port", "port to listen", false, -1, "PORT")
        , Config("", "config", "configuration file", false, "", "FILE")
        , ConfigTemplate("", "config-template", "print configuration file template")
    {
        CmdLine.add(CellNode);
        CmdLine.add(CellMaster);
        CmdLine.add(Scheduler);
        CmdLine.add(JobProxy);
        CmdLine.add(JobId);
        CmdLine.add(Port);
        CmdLine.add(Config);
        CmdLine.add(ConfigTemplate);
    }

    TCLAP::CmdLine CmdLine;

    TCLAP::SwitchArg CellNode;
    TCLAP::SwitchArg CellMaster;
    TCLAP::SwitchArg Scheduler;
    TCLAP::SwitchArg JobProxy;

    TCLAP::ValueArg<Stroka> JobId;
    TCLAP::ValueArg<int> Port;
    TCLAP::ValueArg<Stroka> Config;
    TCLAP::SwitchArg ConfigTemplate;
};

EExitCode GuardedMain(int argc, const char* argv[])
{
    NYT::NThread::SetCurrentThreadName("Bootstrap");

    TArgsParser parser;

    parser.CmdLine.parse(argc, argv);

    // Figure out the mode: cell master, cell node, scheduler or job proxy.
    bool isCellMaster = parser.CellMaster.getValue();
    bool isCellNode = parser.CellNode.getValue();
    bool isScheduler = parser.Scheduler.getValue();
    bool isJobProxy = parser.JobProxy.getValue();

    bool printConfigTemplate = parser.ConfigTemplate.getValue();

    Stroka configFileName = parser.Config.getValue();
    int port = parser.Port.getValue();

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
        TCLAP::StdOutput().usage(parser.CmdLine);
        return EExitCode::OptionsError;
    }

    INodePtr configNode;
    if (!printConfigTemplate) {
        // Configure logging.
        NLog::TLogManager::Get()->Configure(configFileName, "/logging");

        if (configFileName.empty()) {
            THROW_ERROR_EXCEPTION("Missing --config option");
        }

        // Parse configuration file.
        try {
            TIFStream configStream(configFileName);
            configNode = ConvertToNode(&configStream);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error reading server configuration")
                << ex;
        }
    }

    NProfiling::TProfilingManager::Get()->Start();

    // Start an appropriate server.
    if (isCellNode) {
        NYT::NThread::SetCurrentThreadName("NodeMain");

        auto config = New<NCellNode::TCellNodeConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(configNode, false);

            // Override RPC port.
            // TODO(babenko): enable overriding arbitrary options from the command line
            if (port >= 0) {
                config->RpcPort = port;
            }

            config->Validate();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell node configuration")
                << ex;
        }

        NCellNode::TBootstrap bootstrap(configFileName, config);
        bootstrap.Run();
    }

    if (isCellMaster) {
        NYT::NThread::SetCurrentThreadName("MasterMain");

        auto config = New<NCellMaster::TCellMasterConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(configNode, false);

            // Override RPC port.
            if (port >= 0) {
                config->MetaState->Cell->RpcPort = port;
            }

            config->Validate();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell master configuration")
                << ex;
        }

        NCellMaster::TBootstrap bootstrap(configFileName, config);
        bootstrap.Run();
    }

    if (isScheduler) {
        NYT::NThread::SetCurrentThreadName("SchedulerMain");

        auto config = New<NCellScheduler::TCellSchedulerConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        try {
            config->Load(configNode);

            // Override RPC port.
            if (port >= 0) {
                config->RpcPort = port;
            }
            config->Validate();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing cell scheduler configuration")
                << ex;
        }

        NCellScheduler::TBootstrap bootstrap(configFileName, config);
        bootstrap.Run();
    }

    if (isJobProxy) {
        NYT::NThread::SetCurrentThreadName("JobProxyMain");

        auto config = New<NJobProxy::TJobProxyConfig>();
        if (printConfigTemplate) {
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            return EExitCode::OK;
        }

        TJobId jobId;
        try {
            jobId = TGuid::FromString(parser.JobId.getValue());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing job id")
                << ex;
        }

        try {
            config->Load(configNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing job proxy configuration")
                << ex;
        }

        auto jobProxy = New<TJobProxy>(config, jobId);
        jobProxy->Run();
    }

    return EExitCode::OK;
}

int Main(int argc, const char* argv[])
{
    NYT::InstallCrashSignalHandler();

#ifdef _unix_
    sigset_t sigset;
    SigEmptySet(&sigset);
    SigAddSet(&sigset, SIGHUP);
    SigProcMask(SIG_BLOCK, &sigset, NULL);
#endif

    int exitCode;
    try {
        exitCode = GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Server startup failed");
        exitCode = EExitCode::BootstrapError;
    }

    // TODO: refactor system shutdown
    NMetaState::TAsyncChangeLog::Shutdown();
    NLog::TLogManager::Get()->Shutdown();
    NBus::TTcpDispatcher::Get()->Shutdown();
    NRpc::TDispatcher::Get()->Shutdown();
    NChunkClient::TDispatcher::Get()->Shutdown();
    NProfiling::TProfilingManager::Get()->Shutdown();
    TDelayedInvoker::Shutdown();

    return exitCode;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    return NYT::Main(argc, argv);
}
