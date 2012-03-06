#include "stdafx.h"
#include "bootstrap.h"
#include "world_initializer.h"
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
using namespace NScheduler;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Server");

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

TCompositeMetaState* TBootstrap::GetMetaState() const
{
    return ~MetaState;
}

TObjectManager* TBootstrap::GetObjectManager() const
{
    return ~ObjectManager;
}

TChunkManager* TBootstrap::GetChunkManager() const
{
    return ~ChunkManager;
}

IHolderAuthority* TBootstrap::GetHolderAuthority() const
{
    return ~HolderAuthority;
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

    MetaState = New<TCompositeMetaState>();

    ControlQueue = New<TActionQueue>("Control");
    StateQueue = New<TPrioritizedActionQueue>(StateThreadPriorityCount, "MetaState");

    auto busServer = CreateNLBusServer(~New<TNLBusServerConfig>(rpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    MetaStateManager = CreatePersistentStateManager(
        ~Config->MetaState,
        GetControlInvoker(),
        GetStateInvoker(),
        ~MetaState,
        ~rpcServer);

    ObjectManager = New<TObjectManager>(~Config->Objects, this);

    TransactionManager = New<TTransactionManager>(~Config->Transactions, this);

    CypressManager = New<TCypressManager>(this);

    auto cypressService = New<TCypressService>(this);
    rpcServer->RegisterService(~cypressService);

    HolderAuthority = CreateHolderAuthority(this);

    ChunkManager = New<TChunkManager>(~Config->Chunks, this);

    auto chunkService = New<TChunkService>(this);
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

    auto schedulerRedirectorService = CreateRedirectorService(this);
    rpcServer->RegisterService(~schedulerRedirectorService);

    CypressManager->RegisterHandler(~CreateChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateLostChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateOverreplicatedChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateUnderreplicatedChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateChunkListMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateTransactionMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateNodeMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateLockMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateOrchidTypeHandler(this));
    CypressManager->RegisterHandler(~CreateHolderTypeHandler(this));
    CypressManager->RegisterHandler(~CreateHolderMapTypeHandler(this));
    CypressManager->RegisterHandler(~CreateFileTypeHandler(this));
    CypressManager->RegisterHandler(~CreateTableTypeHandler(this));

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
