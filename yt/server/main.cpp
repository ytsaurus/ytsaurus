#include <util/config/last_getopt.h>

#include <yt/ytlib/actions/action_queue.h>
#include <yt/ytlib/rpc/server.h>
#include <yt/ytlib/chunk_holder/chunk_holder.h>
#include <yt/ytlib/chunk_manager/chunk_manager.h>
#include <yt/ytlib/transaction/transaction_manager.h>

using namespace NYT;

using NChunkHolder::TChunkHolderConfig;
using NChunkHolder::TChunkHolder;

using NTransaction::TTransactionManager;

using NChunkManager::TChunkManagerConfig;
using NChunkManager::TChunkManager;

NLog::TLogger Logger("Server");

void RunChunkHolder(const TChunkHolderConfig& config)
{
    LOG_INFO("Starting chunk holder on port %d",
        config.Port);

    IInvoker::TPtr serviceInvoker = new TActionQueue();

    NRpc::TServer::TPtr server = new NRpc::TServer(config.Port);

    TChunkHolder::TPtr chunkHolder = new TChunkHolder(
        config,
        serviceInvoker,
        server);

    server->Start();
}

// TODO: move to a proper place
//! Describes a configuration of TCellMaster.
struct TCellMasterConfig
{
    //! Cell configuration.
    TCellManager::TConfig Cell;

    //! Meta state configuration.
    TMasterStateManager::TConfig MetaState;

    TCellMasterConfig()
    { }

    //! Reads configuration from JSON.
    void Read(TJsonObject* json)
    {
        TJsonObject* cellJson = GetSubTree(json, "Cell");
        if (cellJson != NULL) {
            Cell.Read(cellJson);
        }

        TJsonObject* metaStateJson = GetSubTree(json, "MetaState");
        if (metaStateJson != NULL) {
            MetaState.Read(metaStateJson);
        }
    }
};

void RunCellMaster(const TCellMasterConfig& config)
{
    // TODO: extract method
    Stroka address = config.Cell.MasterAddresses.at(config.Cell.Id);
    size_t index = address.find_last_of(":");
    int port = FromString<int>(address.substr(index + 1));

    LOG_INFO("Starting cell master on port %d", port);

    TCompositeMetaState::TPtr metaState = new TCompositeMetaState();

    IInvoker::TPtr liteInvoker = new TActionQueue();
    IInvoker::TPtr metaStateInvoker = metaState->GetInvoker();

    NRpc::TServer::TPtr server = new NRpc::TServer(port);

    TCellManager::TPtr cellManager = new TCellManager(config.Cell);

    TMasterStateManager::TPtr metaStateManager = new TMasterStateManager(
        config.MetaState,
        cellManager,
        liteInvoker,
        ~metaState,
        server);

    TTransactionManager::TPtr transactionManager = new TTransactionManager(
        TTransactionManager::TConfig(),
        metaStateManager,
        metaState,
        metaStateInvoker,
        server);

    TChunkManager::TPtr chunkManager = new TChunkManager(
        TChunkManagerConfig(),
        metaStateManager,
        metaState,
        metaStateInvoker,
        server,
        transactionManager);

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

        TMasterId masterId = InvalidMasterId;
        opts.AddLongOption("id", "master id")
            .Optional()
            .RequiredArgument("ID")
            .StoreResult(&masterId);

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

            if (masterId >= 0) {
                // TODO: check id

                config.Cell.Id = masterId;
            }

            // TODO: check that config.Cell.Id is initialized
            RunCellMaster(config);
        }

        Cin.ReadLine();

        return 0;
    }
    catch (NStl::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
        return 2;
    }
}

////////////////////////////////////////////////////////////////////////////////
