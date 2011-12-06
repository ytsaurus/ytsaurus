#include "stdafx.h"
#include "cell_master_server.h"

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/virtual.h>

#include <yt/ytlib/meta_state/composite_meta_state.h>
#include <yt/ytlib/meta_state/persistent_state_manager.h>

#include <yt/ytlib/transaction_server/transaction_manager.h>
#include <yt/ytlib/transaction_server/transaction_service.h>
#include <yt/ytlib/transaction_server/cypress_integration.h>

#include <yt/ytlib/cypress/cypress_manager.h>
#include <yt/ytlib/cypress/cypress_service.h>
#include <yt/ytlib/cypress/cypress_integration.h>

#include <yt/ytlib/chunk_server/chunk_manager.h>
#include <yt/ytlib/chunk_server/chunk_service.h>
#include <yt/ytlib/chunk_server/cypress_integration.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/ytree_integration.h>
#include <yt/ytlib/monitoring/http_tree_server.h>

#include <yt/ytlib/orchid/cypress_integration.h>
#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/file_server/file_node.h>

#include <yt/ytlib/table_server/table_node.h>

#include <yt/ytlib/ytree/yson_file_service.h>
#include <yt/ytlib/ytree/ypath_service.h>

#include <yt/ytlib/bus/nl_server.h>

namespace NYT {

static NLog::TLogger Logger("CellMaster");

using NBus::IBusServer;
using NBus::TNLBusServerConfig;
using NBus::CreateNLBusServer;

using NRpc::IRpcServer;
using NRpc::CreateRpcServer;

using NYTree::IYPathService;

using NTransactionServer::TTransactionManager;
using NTransactionServer::TTransactionService;
using NTransactionServer::CreateTransactionMapTypeHandler;

using NChunkServer::TChunkManagerConfig;
using NChunkServer::TChunkManager;
using NChunkServer::TChunkService;
using NChunkServer::CreateChunkMapTypeHandler;
using NChunkServer::CreateChunkListMapTypeHandler;
using NChunkServer::CreateHolderRegistry;
using NChunkServer::CreateHolderMapTypeHandler;

using NMetaState::TCompositeMetaState;
using NMetaState::EPeerStatus;
using NMetaState::IMetaStateManager;

using NCypress::TCypressManager;
using NCypress::TCypressService;
using NCypress::CreateLockMapTypeHandler;
using NCypress::RootNodeId;

using NMonitoring::TMonitoringManager;
using NMonitoring::THttpTreeServer;
using NMonitoring::GetYPathHttpHandler;

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

    auto busServer = CreateNLBusServer(TNLBusServerConfig(port));

    auto rpcServer = CreateRpcServer(~busServer);

    auto metaStateManager = CreatePersistentStateManager(
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

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo));
    monitoringManager->Register(
        "/meta_state",
        FromMethod(&IMetaStateManager::GetMonitoringInfo, metaStateManager));
    monitoringManager->Register(
        "/bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, busServer));
    monitoringManager->Register(
        "/rpc_server",
        FromMethod(&IRpcServer::GetMonitoringInfo, rpcServer));
    monitoringManager->Start();

    // TODO: refactor
    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();  
    orchidRoot->AddChild(
        NYTree::CreateVirtualNode(
            ~NMonitoring::CreateMonitoringProvider(~monitoringManager),
            orchidFactory),
        "monitoring");
    if (!Config.NewConfigFileName.empty()) {
        orchidRoot->AddChild(
            NYTree::CreateVirtualNode(
                ~NYTree::CreateYsonFileProvider(Config.NewConfigFileName),
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
    cypressManager->RegisterNodeType(~CreateHolderTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateHolderMapTypeHandler(
        ~metaStateManager,
        ~cypressManager,
        ~chunkManager));

    cypressManager->RegisterNodeType(~CreateFileTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterNodeType(~CreateTableTypeHandler(
        ~cypressManager,
        ~chunkManager));

    // TODO: fix memory leaking
    auto httpServer = new THttpTreeServer(Config.MonitoringPort);
    auto orchidPathService = ToFuture(IYPathService::FromNode(~orchidRoot));
    httpServer->Register(
        "orchid",
        GetYPathHttpHandler(
            ~FromFunctor([=] () -> TFuture<IYPathService::TPtr>::TPtr
                {
                    return orchidPathService;
                })));
    httpServer->Register(
        "cypress",
        GetYPathHttpHandler(
            ~FromFunctor([=] () -> IYPathService::TPtr
                {
                    auto status = metaStateManager->GetStateStatus();
                    if (status != EPeerStatus::Leading && status != EPeerStatus::Following) {
                        return NULL;
                    }
                    return IYPathService::FromNode(~cypressManager->GetNodeProxy(
                        RootNodeId,
                        NTransactionServer::NullTransactionId));
                })
            ->AsyncVia(metaStateManager->GetStateInvoker())));            

    httpServer->Start();
    metaStateManager->Start();
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
