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

    NRpc::TServer::TPtr server = new NRpc::TServer(
        config.Port,
        serviceInvoker);

    TChunkHolder::TPtr chunkHolder = new TChunkHolder(
        config,
        server);
}

// TODO: move to a proper place
//! Describes a configuration of TCellMaster.
struct TCellMasterConfig
{
    //! Port number to listen.
    int Port;

    TCellMasterConfig()
        : Port(9001)
    { }

    //! Reads configuration from JSON.
    void Read(const TJsonObject* json)
    {
        // TODO
    }
};

void RunMaster(const TCellMasterConfig& config)
{
    LOG_INFO("Starting cell master on port %d",
        config.Port);

    IInvoker::TPtr serviceInvoker = new TActionQueue();

    NRpc::TServer::TPtr server = new NRpc::TServer(
        config.Port,
        serviceInvoker);

    TTransactionManager::TPtr transactionManager = new TTransactionManager(
        TTransactionManager::TConfig(),
        server);

    TChunkManager::TPtr chunkManager = new TChunkManager(
        TChunkManagerConfig(),
        server,
        transactionManager);
}

int main(int argc, const char *argv[])
{
    try {
        using namespace NLastGetopt;
        TOpts opts;

        opts.AddHelpOption();
        
        opts.AddLongOption("chunk-holder", "start chunk holder")
            .NoArgument()
            .Optional();
        
        opts.AddLongOption("master", "start master")
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

        TOptsParseResult results(&opts, argc, argv);

        bool isMaster = results.Has("master");
        bool isChunkHolder = results.Has("chunk-holder");

        int modeCount = 0;
        if (isChunkHolder) {
            ++modeCount;
        }

        if (isMaster) {
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

        if (isMaster) {
            TCellMasterConfig config;
            if (port >= 0) {
                config.Port = port;
            }
            RunMaster(config);
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
