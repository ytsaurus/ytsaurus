#include "stdafx.h"
#include "cell_master_server.h"

#include <yt/ytlib/meta_state/composite_meta_state.h>

#include <yt/ytlib/transaction_manager/transaction_manager.h>
#include <yt/ytlib/transaction_manager/transaction_service.h>

#include <yt/ytlib/cypress/cypress_manager.h>
#include <yt/ytlib/cypress/cypress_service.h>
#include <yt/ytlib/cypress/world_initializer.h>

#include <yt/ytlib/chunk_server/chunk_manager.h>
#include <yt/ytlib/chunk_server/chunk_service.h>
#include <yt/ytlib/chunk_server/cypress_integration.h>

#include <yt/ytlib/file_server/file_manager.h>
#include <yt/ytlib/file_server/file_service.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>

namespace NYT {

static NLog::TLogger Logger("Server");

using NTransaction::TTransactionManager;
using NTransaction::TTransactionService;

using NChunkServer::TChunkManagerConfig;
using NChunkServer::TChunkManager;
using NChunkServer::TChunkService;
using NChunkServer::CreateChunkMapTypeHandler;

using NMetaState::TCompositeMetaState;

using NCypress::TCypressManager;
using NCypress::TCypressService;
using NCypress::TWorldInitializer;

using NFileServer::TFileManager;
using NFileServer::TFileService;

using NMonitoring::TMonitoringManager;

////////////////////////////////////////////////////////////////////////////////

void TCellMasterServer::TConfig::Read(TJsonObject* json)
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

////////////////////////////////////////////////////////////////////////////////

TCellMasterServer::TCellMasterServer(const TConfig& config)
    : Config(config)
{ }

void TCellMasterServer::Run()
{
    // TODO: extract method
    Stroka address = Config.MetaState.Cell.Addresses.at(Config.MetaState.Cell.Id);
    size_t index = address.find_last_of(":");
    int port = FromString<int>(address.substr(index + 1));

    LOG_INFO("Starting cell master on port %d", port);

    auto metaState = New<TCompositeMetaState>();

    auto controlQueue = New<TActionQueue>();

    auto server = New<NRpc::TServer>(port);

    auto metaStateManager = New<TMetaStateManager>(
        Config.MetaState,
        controlQueue->GetInvoker(),
        ~metaState,
        server);

    auto transactionManager = New<TTransactionManager>(
        TTransactionManager::TConfig(),
        metaStateManager,
        metaState);

    auto transactionService = New<TTransactionService>(
        transactionManager,
        metaStateManager->GetStateInvoker(),
        server);

    auto chunkManager = New<TChunkManager>(
        TChunkManagerConfig(),
        ~metaStateManager,
        ~metaState,
        ~transactionManager);

    auto chunkService = New<TChunkService>(
        chunkManager,
        transactionManager,
        metaStateManager->GetStateInvoker(),
        server);

    auto cypressManager = New<TCypressManager>(
        ~metaStateManager,
        ~metaState,
        ~transactionManager);

    auto cypressService = New<TCypressService>(
        ~cypressManager,
        ~transactionManager,
        ~metaStateManager->GetStateInvoker(),
        ~server);

    auto fileManager = New<TFileManager>(
        ~metaStateManager,
        ~metaState,
        ~cypressManager,
        ~chunkManager,
        ~transactionManager);

    auto fileService = New<TFileService>(
        ~chunkManager,
        ~fileManager,
        ~metaStateManager->GetStateInvoker(),
        ~server);

    // TODO: move
    cypressManager->RegisterNodeType(~CreateChunkMapTypeHandler(
        ~cypressManager,
        ~chunkManager));

    auto worldIntializer = New<TWorldInitializer>(
        ~metaStateManager,
        ~cypressManager);
    worldIntializer->Start();

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
        Config.MonitoringPort);

    MonitoringServer->Start();
    metaStateManager->Start();
    server->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
