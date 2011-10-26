#include "stdafx.h"
#include "cell_master_server.h"
#include "chunk_holder_server.h"

#include <yt/ytlib/rpc/server.h>

//using namespace NYT;
namespace NYT {

using NYT::NElection::TPeerId;
using NYT::NElection::InvalidPeerId;

} // namespace NYT

int main(int argc, const char *argv[])
{
    try {
        using namespace NYT;
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
            TChunkHolderServer::TConfig config;
            config.Read(configRoot);
            if (port >= 0) {
                config.Port = port;
            }
            TChunkHolderServer chunkHolderServer(config);
            chunkHolderServer.Run();
        }

        if (isCellMaster) {
            TCellMasterServer::TConfig config;
            config.Read(configRoot);

            if (peerId >= 0) {
                // TODO: check id
                config.MetaState.Cell.Id = peerId;
            }

            // TODO: check that config.Cell.Id is initialized
            TCellMasterServer cellMasterServer(config);
            cellMasterServer.Run();
        }

        return 0;
    }
    catch (std::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
        return 2;
    }
}

