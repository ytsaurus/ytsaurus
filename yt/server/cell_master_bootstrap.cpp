#include "stdafx.h"
#include "cell_master_bootstrap.h"

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/virtual.h>

#include <yt/ytlib/meta_state/composite_meta_state.h>
#include <yt/ytlib/meta_state/persistent_state_manager.h>

#include <yt/ytlib/object_server/object_manager.h>

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
#include <yt/ytlib/monitoring/http_server.h>
#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/statlog.h>

#include <yt/ytlib/orchid/cypress_integration.h>
#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/file_server/file_node.h>

#include <yt/ytlib/table_server/table_node.h>

#include <yt/ytlib/ytree/yson_file_service.h>
#include <yt/ytlib/ytree/ypath_service.h>
#include <yt/ytlib/ytree/ypath_client.h>

#include <yt/ytlib/bus/nl_server.h>

namespace NYT {

static NLog::TLogger Logger("Server");

using NBus::IBusServer;
using NBus::TNLBusServerConfig;
using NBus::CreateNLBusServer;

using NRpc::IRpcServer;
using NRpc::CreateRpcServer;

using NYTree::IYPathService;
using NYTree::SyncYPathSetNode;

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

using NObjectServer::TObjectManager;

using NCypress::TCypressManager;
using NCypress::TCypressService;
using NCypress::CreateLockMapTypeHandler;

using NMonitoring::TMonitoringManager;
using NMonitoring::GetYPathHttpHandler;
using NMonitoring::CreateMonitoringProvider;

using NOrchid::CreateOrchidTypeHandler;

using NFileServer::CreateFileTypeHandler;

using NTableServer::CreateTableTypeHandler;

////////////////////////////////////////////////////////////////////////////////

TCellMasterBootstrap::TCellMasterBootstrap(
    const Stroka& configFileName,
    TConfig* config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

void TCellMasterBootstrap::Run()
{
    // TODO: extract method
    Stroka address = Config->MetaState->Cell->Addresses.at(Config->MetaState->Cell->Id);
    size_t index = address.find_last_of(":");
    int rpcPort = FromString<int>(address.substr(index + 1));

    LOG_INFO("Starting cell master");

    // TODO: fixme
    // Explicitly instrumentation thread creation.
    //NSTAT::EnableStatlog(true);

    auto metaState = New<TCompositeMetaState>();

    auto controlQueue = New<TActionQueue>("Control");

    auto busServer = CreateNLBusServer(~New<TNLBusServerConfig>(rpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    auto metaStateManager = CreateAndRegisterPersistentStateManager(
        ~Config->MetaState,
        ~controlQueue->GetInvoker(),
        ~metaState,
        ~rpcServer);

    auto objectManager = New<TObjectManager>(
        ~metaStateManager,
        ~metaState,
        Config->CellId);

    auto transactionManager = New<TTransactionManager>(
        ~Config->TransactionManager,
        ~metaStateManager,
        ~metaState,
        ~objectManager);

    auto transactionService = New<TTransactionService>(
        ~metaStateManager,
        ~transactionManager);
    rpcServer->RegisterService(~transactionService);

    auto cypressManager = New<TCypressManager>(
        ~metaStateManager,
        ~metaState,
        ~transactionManager,
        ~objectManager);

    auto cypressService = New<TCypressService>(
        ~metaStateManager->GetStateInvoker(),
        ~cypressManager,
        ~transactionManager);
    rpcServer->RegisterService(~cypressService);

    auto holderRegistry = CreateHolderRegistry(~cypressManager);

    auto chunkManager = New<TChunkManager>(
        ~New<TChunkManagerConfig>(),
        ~metaStateManager,
        ~metaState,
        ~transactionManager,
        ~holderRegistry,
        ~objectManager);

    auto chunkService = New<TChunkService>(
        ~metaStateManager,
        ~chunkManager,
        ~transactionManager);
    rpcServer->RegisterService(~chunkService);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
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

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    auto orchidRootService = IYPathService::FromNode(~orchidRoot);
    SyncYPathSetNode(
        ~orchidRootService,
        "/monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProvider(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRootService,
        "/config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProvider(ConfigFileName)));

    auto orchidRpcService = New<NOrchid::TOrchidService>(
        ~orchidRoot,
        ~controlQueue->GetInvoker());
    rpcServer->RegisterService(~orchidRpcService);

    cypressManager->RegisterHandler(~CreateChunkMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateLostChunkMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateOverreplicatedChunkMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateUnderreplicatedChunkMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateChunkListMapTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateTransactionMapTypeHandler(
        ~cypressManager,
        ~transactionManager));
    cypressManager->RegisterHandler(~CreateNodeMapTypeHandler(
        ~cypressManager));
    cypressManager->RegisterHandler(~CreateLockMapTypeHandler(
        ~cypressManager));
    cypressManager->RegisterHandler(~CreateOrchidTypeHandler(
        ~cypressManager));
    cypressManager->RegisterHandler(~CreateHolderTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateHolderMapTypeHandler(
        ~metaStateManager,
        ~cypressManager,
        ~chunkManager));

    cypressManager->RegisterHandler(~CreateFileTypeHandler(
        ~cypressManager,
        ~chunkManager));
    cypressManager->RegisterHandler(~CreateTableTypeHandler(
        ~cypressManager,
        ~chunkManager));

    THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    auto orchidPathService = IYPathService::FromNode(~orchidRoot);
    httpServer->Register(
        "/statistics",
        ~NMonitoring::GetProfilingHttpHandler());
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(
            ~FromFunctor([=] () -> IYPathService::TPtr
                {
                    return orchidPathService;
                }),
            ~metaStateManager->GetStateInvoker()));
    httpServer->Register(
        "/cypress",
        ~NMonitoring::GetYPathHttpHandler(
            ~FromFunctor([=] () -> IYPathService::TPtr
                {
                    auto status = metaStateManager->GetStateStatus();
                    if (status != EPeerStatus::Leading && status != EPeerStatus::Following) {
                        return NULL;
                    }
                    return IYPathService::FromNode(~cypressManager->GetNodeProxy(
                        cypressManager->GetRootNodeId(),
                        NTransactionServer::NullTransactionId));
                }),
            ~metaStateManager->GetStateInvoker()));

    metaStateManager->Start();

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", rpcPort);
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
