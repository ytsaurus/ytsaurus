#include <util/config/last_getopt.h>
#include <util/datetime/base.h>

#include <yt/ytlib/actions/action_queue.h>

#include <yt/ytlib/rpc/server.h>

#include <yt/ytlib/chunk_holder/chunk_holder.h>
#include <yt/ytlib/chunk_manager/chunk_manager.h>

#include <yt/ytlib/transaction/transaction_manager.h>

#include <yt/ytlib/cypress/cypress_state.h>
#include <yt/ytlib/cypress/cypress_service.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/http_tree_server.h>

using namespace NYT;

using NElection::TPeerId;
using NElection::InvalidPeerId;

using NChunkHolder::TChunkHolderConfig;
using NChunkHolder::TChunkHolder;

using NTransaction::TTransactionManager;

using NChunkManager::TChunkManagerConfig;
using NChunkManager::TChunkManager;

using NMetaState::TMetaStateManager;
using NMetaState::TCompositeMetaState;

using NCypress::TCypressState;
using NCypress::TCypressService;

using NMonitoring::TMonitoringManager;
using NMonitoring::THttpTreeServer;

NLog::TLogger Logger("Server");

void RunChunkHolder(const TChunkHolderConfig& config)
{
    LOG_INFO("Starting chunk holder on port %d",
        config.Port);

    auto controlQueue = New<TActionQueue>();

    auto server = New<NRpc::TServer>(config.Port);

    auto chunkHolder = New<TChunkHolder>(
        config,
        controlQueue->GetInvoker(),
        server);

    server->Start();
}

// TODO: move to a proper place
//! Describes a configuration of TCellMaster.
struct TCellMasterConfig
{
    //! Meta state configuration.
    TMetaStateManager::TConfig MetaState;

    int MonitoringPort;

    TCellMasterConfig()
        : MonitoringPort(10000)
    { }

    //! Reads configuration from JSON.
    void Read(TJsonObject* json)
    {
        TJsonObject* cellJson = GetSubTree(json, "Cell");
        if (cellJson != NULL) {
            MetaState.Cell.Read(cellJson);
        }

        TJsonObject* metaStateJson = GetSubTree(json, "MetaState");
        if (metaStateJson != NULL) {
            MetaState.Read(metaStateJson);
        }
    }
};

THttpTreeServer* MonitoringServer; // TODO: encapsulate this

void RunCellMaster(const TCellMasterConfig& config)
{
    // TODO: extract method
    Stroka address = config.MetaState.Cell.Addresses.at(config.MetaState.Cell.Id);
    size_t index = address.find_last_of(":");
    int port = FromString<int>(address.substr(index + 1));

    LOG_INFO("Starting cell master on port %d", port);

    auto metaState = New<TCompositeMetaState>();

    auto controlQueue = New<TActionQueue>();
    auto metaStateInvoker = metaState->GetInvoker();

    auto server = New<NRpc::TServer>(port);

    auto metaStateManager = New<TMetaStateManager>(
        config.MetaState,
        controlQueue->GetInvoker(),
        ~metaState,
        server);

    auto transactionManager = New<TTransactionManager>(
        TTransactionManager::TConfig(),
        metaStateManager,
        metaState,
        metaStateInvoker,
        server);

    auto chunkManager = New<TChunkManager>(
        TChunkManagerConfig(),
        metaStateManager,
        metaState,
        server,
        transactionManager);


    auto cypressState = New<TCypressState>(
        metaStateManager,
        metaState,
        transactionManager);

    auto cypressService = New<TCypressService>(
        TCypressService::TConfig(),
        metaState->GetInvoker(),
        server,
        cypressState);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/refcounted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo));
    monitoringManager->Register(
        "/meta_state",
        FromMethod(&TMetaStateManager::GetMonitoringInfo, metaStateManager));

    // TODO: register more monitoring infos

    monitoringManager->Start();

    MonitoringServer = new THttpTreeServer(
        monitoringManager->GetProducer(),
        config.MonitoringPort);
    
    MonitoringServer->Start();
    metaStateManager->Start();
    server->Start();
}

int main(int argc, const char *argv[])
{
    try {
        using namespace NLastGetopt;
        TOpts opts;

        opts.AddHelpOption();
        
        const TOpt& chunkHolderOpt = opts.AddLongOption("chunk-holder", "start chunk holder")
            .NoArgument()
            .Optional();
        
        const TOpt& cellMasterOpt = opts.AddLongOption("cell-master", "start cell master")
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

        bool isCellMaster = results.Has(&cellMasterOpt);
        bool isChunkHolder = results.Has(&chunkHolderOpt);

        int modeCount = 0;
        if (isChunkHolder) {
            ++modeCount;
        }

        if (isCellMaster) {
            ++modeCount;
        }

        if (modeCount != 1) {
            opts.PrintUsage(results.GetProgramName());
            return 1;
        }

        NLog::TLogManager::Get()->Configure(configFileName, "Logging");

        TIFStream configStream(configFileName);
        TJsonReader configReader(CODES_UTF8, &configStream);
        TJsonObject* configRoot = configReader.ReadAll();

        if (isChunkHolder) {
            NChunkHolder::TChunkHolderConfig config;
            config.Read(configRoot);
            if (port >= 0) {
                config.Port = port;
            }
            RunChunkHolder(config);
        }

        if (isCellMaster) {
            TCellMasterConfig config;
            config.Read(configRoot);

            if (peerId >= 0) {
                // TODO: check id
                config.MetaState.Cell.Id = peerId;
            }

            // TODO: check that config.Cell.Id is initialized
            RunCellMaster(config);
        }

        Sleep(TDuration::Max());

        return 0;
    }
    catch (NStl::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
        return 2;
    }
}

////////////////////////////////////////////////////////////////////////////////
