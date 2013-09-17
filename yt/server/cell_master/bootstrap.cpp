#include "stdafx.h"
#include "bootstrap.h"
#include "meta_state_facade.h"
#include "config.h"

#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/bus/config.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/virtual.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/bus/server.h>
#include <ytlib/bus/tcp_server.h>

#include <ytlib/rpc/server.h>

#include <ytlib/profiling/profiling_manager.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/object_service.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/cypress_integration.h>

#include <server/cypress_server/cypress_manager.h>
#include <server/cypress_server/cypress_integration.h>

#include <server/node_tracker_server/node_tracker_service.h>
#include <server/node_tracker_server/cypress_integration.h>

#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/job_tracker_service.h>
#include <server/chunk_server/chunk_service.h>
#include <server/chunk_server/cypress_integration.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/cypress_integration.h>

#include <server/file_server/file_node.h>

#include <server/table_server/table_node.h>

#include <server/orchid/cypress_integration.h>

#include <server/security_server/security_manager.h>

#include <server/misc/build_attributes.h>

namespace NYT {
namespace NCellMaster {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NMetaState;
using namespace NNodeTrackerServer;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NFileServer;
using namespace NTableServer;
using namespace NSecurityServer;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("MasterBootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellMasterConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

// Neither remove it nor move it to the header.
TBootstrap::~TBootstrap()
{ }

TCellMasterConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IServerPtr TBootstrap::GetRpcServer() const
{
    return RpcServer;
}

TNodeTrackerPtr TBootstrap::GetNodeTracker() const
{
    return NodeTracker;
}

TTransactionManagerPtr TBootstrap::GetTransactionManager() const
{
    return TransactionManager;
}

TCypressManagerPtr TBootstrap::GetCypressManager() const
{
    return CypressManager;
}

TMetaStateFacadePtr TBootstrap::GetMetaStateFacade() const
{
    return MetaStateFacade;
}

TObjectManagerPtr TBootstrap::GetObjectManager() const
{
    return ObjectManager;
}

TChunkManagerPtr TBootstrap::GetChunkManager() const
{
    return ChunkManager;
}

TSecurityManagerPtr TBootstrap::GetSecurityManager() const
{
    return SecurityManager;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue->GetInvoker();
}

void TBootstrap::Run()
{
    LOG_INFO("Starting cell master");

    ControlQueue = New<TActionQueue>("Control");

    auto busServerConfig = New<TTcpBusServerConfig>(Config->MetaState->Cell->RpcPort);
    auto busServer = CreateTcpBusServer(busServerConfig);

    RpcServer = CreateRpcServer(busServer);

    MetaStateFacade = New<TMetaStateFacade>(Config, this);

    // NB: This is exactly the order in which parts get registered and there are some
    // dependencies in Clear methods.
    ObjectManager = New<TObjectManager>(Config->ObjectManager, this);
    SecurityManager = New<TSecurityManager>(Config->SecurityManager, this);
    NodeTracker = New<TNodeTracker>(Config->NodeTracker, this);
    TransactionManager = New<TTransactionManager>(Config->TransactionManager, this);
    CypressManager = New<TCypressManager>(Config->CypressManager, this);
    ChunkManager = New<TChunkManager>(Config->ChunkManager, this);

    ObjectManager->Initialize();
    SecurityManager->Initialize();
    NodeTracker->Initialize();
    TransactionManager->Inititialize();
    CypressManager->Initialize();
    ChunkManager->Initialize();

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        BIND(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "/meta_state",
        BIND(&IMetaStateManager::GetMonitoringInfo, MetaStateFacade->GetManager()));

    auto orchidFactory = GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(monitoringManager->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfilingManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        CreateVirtualNode(CreateYsonFileService(ConfigFileName)));
    
    SetBuildAttributes(orchidRoot, "master");

    RpcServer->RegisterService(New<TObjectService>(Config->ObjectManager, this));
    RpcServer->RegisterService(New<TNodeTrackerService>(Config->NodeTracker, this));
    RpcServer->RegisterService(New<TOrchidService>(orchidRoot, GetControlInvoker()));
    RpcServer->RegisterService(CreateJobTrackerService(this));
    RpcServer->RegisterService(CreateChunkService(this));

    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostVitalChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::UnderreplicatedChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::OverreplicatedChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::DataMissingChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ParityMissingChunkMap));
    CypressManager->RegisterHandler(CreateChunkListMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateTransactionMapTypeHandler(this, EObjectType::TransactionMap));
    CypressManager->RegisterHandler(CreateTransactionMapTypeHandler(this, EObjectType::TopmostTransactionMap));
    CypressManager->RegisterHandler(CreateLockMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateOrchidTypeHandler(this));
    CypressManager->RegisterHandler(CreateCellNodeTypeHandler(this));
    CypressManager->RegisterHandler(CreateCellNodeMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateFileTypeHandler(this));
    CypressManager->RegisterHandler(CreateTableTypeHandler(this));
    CypressManager->RegisterHandler(CreateAccountMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateUserMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateGroupMapTypeHandler(this));

    MetaStateFacade->Start();

    monitoringManager->Start();

    NHttp::TServer httpServer(Config->MonitoringPort);
    httpServer.Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(orchidRoot->Via(GetControlInvoker())));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer.Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->MetaState->Cell->RpcPort);
    RpcServer->Configure(Config->RpcServer);
    RpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
