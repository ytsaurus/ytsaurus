#include "stdafx.h"
#include "bootstrap.h"

#include "config.h"

#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/virtual.h>

#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/persistent_state_manager.h>

#include <ytlib/object_server/object_manager.h>

#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/cypress_integration.h>

#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/cypress/cypress_service.h>
#include <ytlib/cypress/cypress_integration.h>
#include <ytlib/cypress/world_initializer.h>

#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/chunk_server/chunk_service.h>
#include <ytlib/chunk_server/cypress_integration.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/orchid/cypress_integration.h>
#include <ytlib/orchid/orchid_service.h>

#include <ytlib/file_server/file_node.h>

#include <ytlib/table_server/table_node.h>

#include <ytlib/scheduler/redirector_service.h>

#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/bus/nl_server.h>

#include <ytlib/profiling/profiling_manager.h>

namespace NYT {
namespace NCellMaster {

static NLog::TLogger Logger("Server");

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NMetaState;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NCypress;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NFileServer;
using namespace NTableServer;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellMasterConfig* config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

// Neither remove it nor move it to header.
TBootstrap::~TBootstrap()
{ }

TCellMasterConfig* TBootstrap::GetConfig() const
{
    return ~Config;
}

TTransactionManager* TBootstrap::GetTransactionManager() const
{
    return ~TransactionManager;
}

TCypressManager* TBootstrap::GetCypressManager() const
{
    return ~CypressManager;
}

TWorldInitializer* TBootstrap::GetWorldInitializer() const
{
    return ~WorldInitializer;
}

IMetaStateManager* TBootstrap::GetMetaStateManager() const
{
    return ~MetaStateManager;
}

IInvoker* TBootstrap::GetControlInvoker()
{
    return ControlQueue->GetInvoker();
}

IInvoker* TBootstrap::GetStateInvoker(EStateThreadPriority priority)
{
    return StateQueue->GetInvoker(priority.ToValue());
}

void TBootstrap::Run()
{
    // TODO: extract method
    Stroka address = Config->MetaState->Cell->Addresses.at(Config->MetaState->Cell->Id);
    size_t index = address.find_last_of(":");
    int rpcPort = FromString<int>(address.substr(index + 1));

    LOG_INFO("Starting cell master");

    auto metaState = New<TCompositeMetaState>();

    ControlQueue = New<TActionQueue>("Control");
    StateQueue = New<TPrioritizedActionQueue>(StateThreadPriorityCount, "MetaState");

    auto busServer = CreateNLBusServer(~New<TNLBusServerConfig>(rpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    MetaStateManager = CreatePersistentStateManager(
        ~Config->MetaState,
        GetControlInvoker(),
        GetStateInvoker(),
        ~metaState,
        ~rpcServer);

    auto objectManager = New<TObjectManager>(
        ~MetaStateManager,
        ~metaState,
        Config->CellId);

    TransactionManager = New<TTransactionManager>(
        ~Config->Transactions,
        ~MetaStateManager,
        ~metaState,
        ~objectManager);
    objectManager->SetTransactionManager(~TransactionManager);

    CypressManager = New<TCypressManager>(
        ~MetaStateManager,
        ~metaState,
        ~TransactionManager,
        ~objectManager);
    objectManager->SetCypressManager(~CypressManager);

    auto cypressService = New<TCypressService>(
        ~MetaStateManager,
        ~objectManager);
    rpcServer->RegisterService(~cypressService);

    auto holderRegistry = CreateHolderAuthority(~CypressManager);

    auto chunkManager = New<TChunkManager>(
        ~New<TChunkManagerConfig>(),
        ~MetaStateManager,
        ~metaState,
        ~TransactionManager,
        ~holderRegistry,
        ~objectManager);

    auto chunkService = New<TChunkService>(
        ~MetaStateManager,
        ~chunkManager,
        ~TransactionManager);
    rpcServer->RegisterService(~chunkService);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "meta_state",
        FromMethod(&IMetaStateManager::GetMonitoringInfo, MetaStateManager));
    monitoringManager->Register(
        "bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, busServer));
    monitoringManager->Start();

    auto orchidFactory = GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SyncYPathSetNode(
        ~orchidRoot,
        "monitoring",
        ~CreateVirtualNode(CreateMonitoringProducer(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRoot,
        "profiling",
        ~CreateVirtualNode(
            ~TProfilingManager::Get()->GetRoot()
            ->Via(TProfilingManager::Get()->GetInvoker())));
    SyncYPathSetNode(
        ~orchidRoot,
        "config",
        ~CreateVirtualNode(~CreateYsonFileProducer(ConfigFileName)));

    auto orchidRpcService = New<NOrchid::TOrchidService>(
        ~orchidRoot,
        GetControlInvoker());
    rpcServer->RegisterService(~orchidRpcService);

    auto schedulerRedirectorService = New<NScheduler::TRedirectorService>(~CypressManager);
    rpcServer->RegisterService(~schedulerRedirectorService);

    CypressManager->RegisterHandler(~CreateChunkMapTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateLostChunkMapTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateOverreplicatedChunkMapTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateUnderreplicatedChunkMapTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateChunkListMapTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateTransactionMapTypeHandler(
        ~CypressManager,
        ~TransactionManager));
    CypressManager->RegisterHandler(~CreateNodeMapTypeHandler(
        ~CypressManager));
    CypressManager->RegisterHandler(~CreateLockMapTypeHandler(
        ~CypressManager));
    CypressManager->RegisterHandler(~CreateOrchidTypeHandler(
        ~CypressManager));
    CypressManager->RegisterHandler(~CreateHolderTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateHolderMapTypeHandler(
        ~MetaStateManager,
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateFileTypeHandler(
        ~CypressManager,
        ~chunkManager));
    CypressManager->RegisterHandler(~CreateTableTypeHandler(
        ~CypressManager,
        ~chunkManager));

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(~orchidRoot->Via(GetControlInvoker())));
    httpServer->Register(
        "/cypress",
        ~NMonitoring::GetYPathHttpHandler(CypressManager->GetRootServiceProducer()));

    MetaStateManager->Start();

    WorldInitializer = New<TWorldInitializer>(this);

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", rpcPort);
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
