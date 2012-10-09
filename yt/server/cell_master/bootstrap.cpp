#include "stdafx.h"
#include "bootstrap.h"
#include "meta_state_facade.h"
#include "config.h"

#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/bus/config.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/virtual.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/object_service.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/cypress_integration.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/chunk_service.h>
#include <server/chunk_server/cypress_integration.h>
#include <server/chunk_server/node_authority.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/bus/tcp_server.h>

#include <ytlib/rpc/server.h>

#include <ytlib/profiling/profiling_manager.h>

#include <server/file_server/file_node.h>

#include <server/table_server/table_node.h>

#include <server/orchid/cypress_integration.h>

#include <yt/build.h>

namespace NYT {
namespace NCellMaster {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NMetaState;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NFileServer;
using namespace NTableServer;
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

INodeAuthorityPtr TBootstrap::GetNodeAuthority() const
{
    return HolderAuthority;
}

IInvokerPtr TBootstrap::GetControlInvoker()
{
    return ControlQueue->GetInvoker();
}

void TBootstrap::Run()
{
    LOG_INFO("Starting cell master");

    ControlQueue = New<TActionQueue>("Control");

    auto busServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->MetaState->Cell->RpcPort));
    RpcServer = CreateRpcServer(busServer);

    MetaStateFacade = New<TMetaStateFacade>(Config, this);

    TransactionManager = New<TTransactionManager>(Config->Transactions, this);

    ObjectManager = New<TObjectManager>(Config->Objects, this);

    CypressManager = New<TCypressManager>(this);

    // TODO(babenko): refactor
    TransactionManager->Init();

    auto objectService = New<TObjectService>(this);
    RpcServer->RegisterService(objectService);

    HolderAuthority = CreateNodeAuthority(this);

    ChunkManager = New<TChunkManager>(Config->Chunks, this);

    auto chunkService = New<TChunkService>(this);
    RpcServer->RegisterService(chunkService);

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
        CreateVirtualNode(CreateMonitoringProducer(monitoringManager)));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(
            TProfilingManager::Get()->GetRoot()
            ->Via(TProfilingManager::Get()->GetInvoker())));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        CreateVirtualNode(CreateYsonFileProducer(ConfigFileName)));

    SyncYPathSet(orchidRoot, "/@service_name", ConvertToYsonString("master"));
    SyncYPathSet(orchidRoot, "/@version", ConvertToYsonString(YT_VERSION));
    SyncYPathSet(orchidRoot, "/@build_host", ConvertToYsonString(YT_BUILD_HOST));
    SyncYPathSet(orchidRoot, "/@build_time", ConvertToYsonString(YT_BUILD_TIME));
    SyncYPathSet(orchidRoot, "/@build_machine", ConvertToYsonString(YT_BUILD_MACHINE));
    SyncYPathSet(orchidRoot, "/@start_time", ConvertToYsonString(TInstant::Now()));

    auto orchidRpcService = New<NOrchid::TOrchidService>(
        orchidRoot,
        GetControlInvoker());
    RpcServer->RegisterService(orchidRpcService);

    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateLostChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateOverreplicatedChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateUnderreplicatedChunkMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateChunkListMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateTransactionMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateOrchidTypeHandler(this));
    CypressManager->RegisterHandler(CreateNodeTypeHandler(this));
    CypressManager->RegisterHandler(CreateNodeMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateFileTypeHandler(this));
    CypressManager->RegisterHandler(CreateTableTypeHandler(this));

    MetaStateFacade->Start();
    monitoringManager->Start();

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(orchidRoot->Via(GetControlInvoker())));
    httpServer->Register(
        "/cypress",
        NMonitoring::GetYPathHttpHandler(CypressManager->GetRootService()));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->MetaState->Cell->RpcPort);
    RpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
