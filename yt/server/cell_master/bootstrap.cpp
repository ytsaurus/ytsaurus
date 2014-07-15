#include "stdafx.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "config.h"

#include <core/misc/ref_counted_tracker.h>

#include <core/bus/config.h>

#include <core/ytree/tree_builder.h>
#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/virtual.h>

#include <core/ytree/yson_file_service.h>
#include <core/ytree/ypath_service.h>
#include <core/ytree/ypath_client.h>

#include <core/bus/server.h>
#include <core/bus/tcp_server.h>

#include <core/rpc/server.h>
#include <core/rpc/bus_server.h>
#include <core/rpc/bus_channel.h>

#include <core/profiling/profiling_manager.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/transaction_client/timestamp_provider.h>
#include <ytlib/transaction_client/remote_timestamp_provider.h>

#include <ytlib/hive/cell_directory.h>

#include <server/hydra/changelog.h>
#include <server/hydra/local_changelog_store.h>
#include <server/hydra/snapshot.h>
#include <server/hydra/file_snapshot_store.h>
#include <server/hydra/local_snapshot_service.h>
#include <server/hydra/local_snapshot_store.h>

#include <server/hive/hive_manager.h>
#include <server/hive/transaction_manager.h>
#include <server/hive/transaction_supervisor.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/object_service.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/cypress_integration.h>
#include <server/transaction_server/timestamp_manager.h>

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

#include <server/journal_server/journal_node.h>

#include <server/orchid/cypress_integration.h>

#include <server/security_server/security_manager.h>

#include <server/tablet_server/tablet_manager.h>
#include <server/tablet_server/cypress_integration.h>

#include <server/misc/build_attributes.h>

namespace NYT {
namespace NCellMaster {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NElection;
using namespace NHydra;
using namespace NHive;
using namespace NNodeTrackerServer;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NJournalServer;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NFileServer;
using namespace NTableServer;
using namespace NJournalServer;
using namespace NSecurityServer;
using namespace NTabletServer;

using NElection::TCellGuid;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");

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

const TCellGuid& TBootstrap::GetCellGuid() const
{
    return Config->Master->CellGuid;
}

ui16 TBootstrap::GetCellId() const
{
    return Config->Master->CellId;
}

TCellMasterConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IServerPtr TBootstrap::GetRpcServer() const
{
    return RpcServer;
}

TCellManagerPtr TBootstrap::GetCellManager() const
{
    return CellManager;
}

IChangelogStorePtr TBootstrap::GetChangelogStore() const
{
    return ChangelogStore;
}

ISnapshotStorePtr TBootstrap::GetSnapshotStore() const
{
    return SnapshotStore;
}

TNodeTrackerPtr TBootstrap::GetNodeTracker() const
{
    return NodeTracker;
}

TTransactionManagerPtr TBootstrap::GetTransactionManager() const
{
    return TransactionManager;
}

TTransactionSupervisorPtr TBootstrap::GetTransactionSupervisor() const
{
    return TransactionSupervisor;
}

TCypressManagerPtr TBootstrap::GetCypressManager() const
{
    return CypressManager;
}

THydraFacadePtr TBootstrap::GetHydraFacade() const
{
    return HydraManager;
}

TWorldInitializerPtr TBootstrap::GetWorldInitializer() const
{
    return WorldInitializer;
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

TTabletManagerPtr TBootstrap::GetTabletManager() const
{
    return TabletManager;
}

THiveManagerPtr TBootstrap::GetHiveManager() const
{
    return HiveManager;
}

TCellDirectoryPtr TBootstrap::GetCellDirectory() const
{
    return CellDirectory;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue->GetInvoker();
}

void TBootstrap::Run()
{
    srand(time(nullptr));

    ControlQueue = New<TActionQueue>("Control");

    auto result = BIND(&TBootstrap::DoRun, this)
        .Guarded()
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get();
    THROW_ERROR_EXCEPTION_IF_FAILED(result);

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    LOG_INFO("Starting cell master (CellGuid: %s, CellId: %d)",
        ~ToString(GetCellGuid()),
        static_cast<int>(GetCellId()));

    if (GetCellGuid() == TCellGuid()) {
        LOG_ERROR("No custom cell GUID is set, cluster can only be used for testing purposes");
    }
    if (GetCellId() == 0) {
        LOG_ERROR("No custom cell ID is set, cluster can only be used for testing purposes");
    }

    auto busServerConfig = New<TTcpBusServerConfig>(Config->RpcPort);
    auto busServer = CreateTcpBusServer(busServerConfig);

    RpcServer = CreateBusServer(busServer);

    HttpServer.reset(new NHttp::TServer(Config->MonitoringPort));

    auto selfAddress = BuildServiceAddress(
        TAddressResolver::Get()->GetLocalHostName(),
        Config->RpcPort);

    const auto& addresses = Config->Master->Addresses;

    auto selfId = std::distance(
        addresses.begin(),
        std::find(addresses.begin(), addresses.end(), selfAddress));

    if (selfId == addresses.size()) {
        THROW_ERROR_EXCEPTION("Missing self address %s is the peer list",
            ~selfAddress.Quote());
    }

    CellManager = New<TCellManager>(
        Config->Master,
        GetBusChannelFactory(),
        selfId);

    ChangelogStore = CreateLocalChangelogStore(
        "ChangelogFlush",
        Config->Changelogs);

    auto fileSnapshotStore = New<TFileSnapshotStore>(
        Config->Snapshots);

    SnapshotStore = CreateLocalSnapshotStore(
        Config->HydraManager,
        CellManager,
        fileSnapshotStore);

    HydraManager = New<THydraFacade>(Config, this);

    WorldInitializer = New<TWorldInitializer>(Config, this);
    
    CellDirectory = New<TCellDirectory>(
        Config->CellDirectory,
        GetBusChannelFactory());
    CellDirectory->RegisterCell(Config->Master);

    HiveManager = New<THiveManager>(
        GetCellGuid(),
        Config->HiveManager,
        CellDirectory,
        HydraManager->GetAutomatonInvoker(),
        HydraManager->GetHydraManager(),
        HydraManager->GetAutomaton());

    // NB: This is exactly the order in which parts get registered and there are some
    // dependencies in Clear methods.
    ObjectManager = New<TObjectManager>(Config->ObjectManager, this);

    SecurityManager = New<TSecurityManager>(Config->SecurityManager, this);
    
    NodeTracker = New<TNodeTracker>(Config->NodeTracker, this);
    
    TransactionManager = New<TTransactionManager>(Config->TransactionManager, this);
    
    CypressManager = New<TCypressManager>(Config->CypressManager, this);
    
    ChunkManager = New<TChunkManager>(Config->ChunkManager, this);

    TabletManager = New<TTabletManager>(Config->TabletManager, this);
    
    auto timestampManager = New<TTimestampManager>(
        Config->TimestampManager,
        HydraManager->GetAutomatonInvoker(),
        HydraManager->GetHydraManager(),
        HydraManager->GetAutomaton());
    
    auto timestampProvider = CreateRemoteTimestampProvider(
        Config->TimestampProvider,
        GetBusChannelFactory());
    
    TransactionSupervisor = New<TTransactionSupervisor>(
        Config->TransactionSupervisor,
        HydraManager->GetAutomatonInvoker(),
        HydraManager->GetHydraManager(),
        HydraManager->GetAutomaton(),
        HiveManager,
        TransactionManager,
        timestampProvider);

    fileSnapshotStore->Initialize();
    ObjectManager->Initialize();
    SecurityManager->Initialize();
    NodeTracker->Initialize();
    TransactionManager->Initialize();
    CypressManager->Initialize();
    ChunkManager->Initialize();
    TabletManager->Initialize();

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        TRefCountedTracker::Get()->GetMonitoringProducer());
    monitoringManager->Register(
        "/hydra",
        HydraManager->GetHydraManager()->GetMonitoringProducer());

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

    RpcServer->RegisterService(timestampManager->GetRpcService());
    RpcServer->RegisterService(HiveManager->GetRpcService());
    RpcServer->RegisterService(TransactionSupervisor->GetRpcService());
    RpcServer->RegisterService(New<TLocalSnapshotService>(GetCellGuid(), fileSnapshotStore));
    RpcServer->RegisterService(CreateObjectService(Config->ObjectManager, this));
    RpcServer->RegisterService(New<TNodeTrackerService>(Config->NodeTracker, this));
    RpcServer->RegisterService(CreateOrchidService(orchidRoot, GetControlInvoker()));
    RpcServer->RegisterService(CreateJobTrackerService(this));
    RpcServer->RegisterService(CreateChunkService(this));

    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostVitalChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::UnderreplicatedChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::OverreplicatedChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::DataMissingChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ParityMissingChunkMap));
    CypressManager->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::QuorumMissingChunkMap));
    CypressManager->RegisterHandler(CreateChunkListMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateTransactionMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateTopmostTransactionMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateLockMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateOrchidTypeHandler(this));
    CypressManager->RegisterHandler(CreateCellNodeTypeHandler(this));
    CypressManager->RegisterHandler(CreateCellNodeMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateFileTypeHandler(this));
    CypressManager->RegisterHandler(CreateTableTypeHandler(this));
    CypressManager->RegisterHandler(CreateJournalTypeHandler(this));
    CypressManager->RegisterHandler(CreateAccountMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateUserMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateGroupMapTypeHandler(this));
    CypressManager->RegisterHandler(CreateTabletCellNodeTypeHandler(this));
    CypressManager->RegisterHandler(CreateTabletMapTypeHandler(this));

    HydraManager->Start();

    monitoringManager->Start();

    HttpServer->Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(orchidRoot->Via(GetControlInvoker())));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    HttpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    RpcServer->Configure(Config->RpcServer);
    RpcServer->Start();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
