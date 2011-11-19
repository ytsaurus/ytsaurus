#include "stdafx.h"
#include "cell_master_server.h"

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/virtual.h>

#include <yt/ytlib/meta_state/composite_meta_state.h>

#include <yt/ytlib/transaction_server/transaction_manager.h>
#include <yt/ytlib/transaction_server/transaction_service.h>
#include <yt/ytlib/transaction_server/cypress_integration.h>

#include <yt/ytlib/cypress/cypress_manager.h>
#include <yt/ytlib/cypress/cypress_service.h>
#include <yt/ytlib/cypress/world_initializer.h>
#include <yt/ytlib/cypress/cypress_integration.h>

#include <yt/ytlib/chunk_server/chunk_manager.h>
#include <yt/ytlib/chunk_server/chunk_service.h>
#include <yt/ytlib/chunk_server/cypress_integration.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/ytree_integration.h>

#include <yt/ytlib/orchid/cypress_integration.h>
#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/file_server/file_node.h>

#include <yt/ytlib/table_server/table_node.h>

#include <yt/ytlib/ytree/yson_file_service.h>

namespace NYT {

static NLog::TLogger Logger("CellMaster");

using NRpc::CreateRpcServer;

using NTransaction::TTransactionManager;
using NTransaction::TTransactionService;
using NTransaction::CreateTransactionMapTypeHandler;

using NChunkServer::TChunkManagerConfig;
using NChunkServer::TChunkManager;
using NChunkServer::TChunkService;
using NChunkServer::CreateChunkMapTypeHandler;
using NChunkServer::CreateChunkListMapTypeHandler;
using NChunkServer::CreateHolderRegistry;
using NChunkServer::CreateHolderMapTypeHandler;

using NMetaState::TCompositeMetaState;

using NCypress::TCypressManager;
using NCypress::TCypressService;
using NCypress::TWorldInitializer;
using NCypress::CreateLockMapTypeHandler;

using NMonitoring::TMonitoringManager;

using NOrchid::CreateOrchidTypeHandler;

using NFileServer::CreateFileTypeHandler;

using NTableServer::CreateTableTypeHandler;

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

    auto rpcServer = CreateRpcServer(port);

    auto metaStateManager = New<TMetaStateManager>(
        Config.MetaState,
        ~controlQueue->GetInvoker(),
        ~metaState,
        ~rpcServer);

    auto transactionManager = New<TTransactionManager>(
        TTransactionManager::TConfig(),
        metaStateManager,
        metaState);

    auto transactionService = New<TTransactionService>(
        ~metaStateManager,
        ~transactionManager,
        ~rpcServer);

    auto cypressManager = New<TCypressManager>(
        ~metaStateManager,
        ~metaState,
        ~transactionManager);

    auto cypressService = New<TCypressService>(
        ~metaStateManager->GetStateInvoker(),
        ~cypressManager,
        ~transactionManager,
        ~rpcServer);

    auto holderRegistry = CreateHolderRegistry(~cypressManager);

    auto chunkManager = New<TChunkManager>(
        TChunkManagerConfig(),
        ~metaStateManager,
        ~metaState,
        ~transactionManager,
        ~holderRegistry);

    auto chunkService = New<TChunkService>(
        ~metaStateManager,
        ~chunkManager,
        ~transactionManager,
        ~rpcServer);

    auto worldIntializer = New<TWorldInitializer>(
        ~metaStateManager,
        ~cypressManager);
    worldIntializer->Start();

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo));
    monitoringManager->Register(
        "/meta_state",
        FromMethod(&TMetaStateManager::GetMonitoringInfo, metaStateManager));
    // TODO: register more monitoring infos
    monitoringManager->Start();

    // TODO: refactor
    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();  
    orchidRoot->AddChild(
        NYTree::CreateVirtualNode(
            ~NMonitoring::CreateMonitoringProducer(~monitoringManager),
            orchidFactory),
        "monitoring");
    if (!Config.NewConfigFileName.empty()) {
        orchidRoot->AddChild(
            NYTree::CreateVirtualNode(
                ~NYTree::CreateYsonFileProducer(Config.NewConfigFileName),
                orchidFactory),
            "config");
    }

    auto orchidService = New<NOrchid::TOrchidService>(
        ~orchidRoot,
        ~rpcServer,
        ~controlQueue->GetInvoker());

    cypressManager->RegisterNodeType(~CreateChunkMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateChunkListMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateTransactionMapTypeHandler(
        ~cypressManager,
        ~transactionManager));
    cypressManager->RegisterNodeType(~CreateNodeMapTypeHandler(
        ~cypressManager));
    cypressManager->RegisterNodeType(~CreateLockMapTypeHandler(
        ~cypressManager));
    cypressManager->RegisterNodeType(~CreateOrchidTypeHandler(
        ~cypressManager));
    cypressManager->RegisterNodeType(~CreateFileTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateTableTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateHolderTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateHolderMapTypeHandler(
        ~cypressManager,
        ~chunkManager));

    auto monitoringServer = new THttpTreeServer(
        monitoringManager->GetProducer(),
        Config.MonitoringPort);

    monitoringServer->Start();
    metaStateManager->Start();
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
