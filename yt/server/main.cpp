#include "stdafx.h"
#include "cell_master_bootstrap.h"
#include "chunk_holder_bootstrap.h"
#include "scheduler_bootstrap.h"

#include <yt/ytlib/misc/enum.h>
#include <yt/ytlib/logging/log_manager.h>
#include <yt/ytlib/ytree/serialize.h>

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

    const auto& chunkHolderOpt = opts.AddLongOption("chunk-holder", "start chunk holder")
        .NoArgument()
        .Optional();

    const auto& cellMasterOpt = opts.AddLongOption("cell-master", "start cell master")
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

    TPeerId peerId = InvalidPeerId;
    opts.AddLongOption("id", "peer id")
        .Optional()
        .RequiredArgument("ID")
        .StoreResult(&peerId);

    Stroka configFileName;
    opts.AddLongOption("config", "configuration file")
        .RequiredArgument("FILE")
        .StoreResult(&configFileName);

    TOptsParseResult results(&opts, argc, argv);

    // Figure out the mode: cell master or chunk holder.
    bool isCellMaster = results.Has(&cellMasterOpt);
    bool isChunkHolder = results.Has(&chunkHolderOpt);
    bool isScheduler = results.Has(&schedulerOpt);

    int modeCount = 0;
    if (isChunkHolder) {
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
    NLog::TLogManager::Get()->Configure(configFileName, "/logging");

    // Parse configuration file.
    INode::TPtr configNode;
    try {
        TIFStream configStream(configFileName);
        configNode = DeserializeFromYson(&configStream);
    } catch (...) {
        ythrow yexception() << Sprintf("Error reading server configuration\n%s",
            ~CurrentExceptionMessage());
    }

    // Start an appropriate server.
    if (isChunkHolder) {
        auto config = New<TChunkHolderBootstrap::TConfig>();
        try {
            config->LoadAndValidate(~configNode);
        } catch (...) {
            ythrow yexception() << Sprintf("Error parsing chunk holder configuration\n%s",
                ~CurrentExceptionMessage());
        }

        //// TODO: killme
        //auto c = New<NChunkHolder::TLocationConfig>();
        //c->Path = NFS::CombinePaths(NFS::GetDirectoryName(config->CacheLocation->Path), "chunk_storage.0");
        //config->StorageLocations.push_back(c);

        // Override RPC port.
        if (port >= 0) {
            config->RpcPort = port;
        }

        TChunkHolderBootstrap chunkHolderBootstrap(configFileName, ~config);
        chunkHolderBootstrap.Run();
    }

    if (isCellMaster) {
        auto config = New<TCellMasterBootstrap::TConfig>();
        try {
            config->Load(~configNode);
            
            // Override peer id.
            if (peerId != InvalidPeerId) {
                config->MetaState->Cell->Id = peerId;
            }

            config->Validate();
        } catch (...) {
            ythrow yexception() << Sprintf("Error parsing cell master configuration\n%s",
                ~CurrentExceptionMessage());
        }

        TCellMasterBootstrap cellMasterBootstrap(configFileName, ~config);
        cellMasterBootstrap.Run();
    }

    if (isScheduler) {
        auto config = New<TSchedulerBootstrap::TConfig>();
        try {
            config->LoadAndValidate(~configNode);
        } catch (...) {
            ythrow yexception() << Sprintf("Error parsing cell master configuration\n%s",
                ~CurrentExceptionMessage());
        }

        TSchedulerBootstrap schedulerBootstrap(configFileName, ~config);
        schedulerBootstrap.Run();
    }

    // Actually this will never happen.
    return EExitCode::OK;
}

int Main(int argc, const char* argv[])
{
    int exitCode;
    try {
        exitCode = GuardedMain(argc, argv);
    }
    catch (...) {
        LOG_ERROR("Server startup failed\n%s", ~CurrentExceptionMessage());
        exitCode = EExitCode::BootstrapError;
    }

    // TODO: refactor system shutdown
    NLog::TLogManager::Get()->Shutdown();
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
