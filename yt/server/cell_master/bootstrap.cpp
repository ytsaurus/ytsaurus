#include "stdafx.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "multicell_manager.h"
#include "config.h"
#include "private.h"

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

#include <core/profiling/profile_manager.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/election/cell_manager.h>

#include <ytlib/object_client/helpers.h>

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
#include <server/hive/cell_directory_synchronizer.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/object_service.h>
#include <server/object_server/sys_node.h>

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
using namespace NObjectClient;
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

using NElection::TCellId;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellMasterConfigPtr config)
    : ConfigFileName_(configFileName)
    , Config_(config)
{ }

// Neither remove it nor move it to the header.
TBootstrap::~TBootstrap()
{ }

TCellMasterConfigPtr TBootstrap::GetConfig() const
{
    return Config_;
}

bool TBootstrap::IsPrimaryMaster() const
{
    return PrimaryMaster_;
}

bool TBootstrap::IsSecondaryMaster() const
{
    return SecondaryMaster_;
}

bool TBootstrap::IsMulticell() const
{
    return Multicell_;
}

const TCellId& TBootstrap::GetCellId() const
{
    return CellId_;
}

TCellTag TBootstrap::GetCellTag() const
{
    return CellTag_;
}

const TCellId& TBootstrap::GetPrimaryCellId() const
{
    return PrimaryCellId_;
}

TCellTag TBootstrap::GetPrimaryCellTag() const
{
    return PrimaryCellTag_;
}

TCellId TBootstrap::GetSecondaryCellId(TCellTag cellTag) const
{
    return ReplaceCellTagInId(PrimaryCellId_, cellTag);
}

const std::vector<TCellTag>& TBootstrap::GetSecondaryCellTags() const
{
    return SecondaryCellTags_;
}

TMulticellManagerPtr TBootstrap::GetMulticellManager() const
{
    return MulticellManager_;
}

IServerPtr TBootstrap::GetRpcServer() const
{
    return RpcServer_;
}

TCellManagerPtr TBootstrap::GetCellManager() const
{
    return CellManager_;
}

IChangelogStorePtr TBootstrap::GetChangelogStore() const
{
    return ChangelogStore_;
}

ISnapshotStorePtr TBootstrap::GetSnapshotStore() const
{
    return SnapshotStore_;
}

TNodeTrackerPtr TBootstrap::GetNodeTracker() const
{
    return NodeTracker_;
}

TTransactionManagerPtr TBootstrap::GetTransactionManager() const
{
    return TransactionManager_;
}

TTransactionSupervisorPtr TBootstrap::GetTransactionSupervisor() const
{
    return TransactionSupervisor_;
}

TCypressManagerPtr TBootstrap::GetCypressManager() const
{
    return CypressManager_;
}

THydraFacadePtr TBootstrap::GetHydraFacade() const
{
    return HydraFacade_;
}

TWorldInitializerPtr TBootstrap::GetWorldInitializer() const
{
    return WorldInitializer_;
}

TObjectManagerPtr TBootstrap::GetObjectManager() const
{
    return ObjectManager_;
}

TChunkManagerPtr TBootstrap::GetChunkManager() const
{
    return ChunkManager_;
}

TSecurityManagerPtr TBootstrap::GetSecurityManager() const
{
    return SecurityManager_;
}

TTabletManagerPtr TBootstrap::GetTabletManager() const
{
    return TabletManager_;
}

THiveManagerPtr TBootstrap::GetHiveManager() const
{
    return HiveManager_;
}

TCellDirectoryPtr TBootstrap::GetCellDirectory() const
{
    return CellDirectory_;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

void TBootstrap::Initialize()
{
    srand(time(nullptr));

    ControlQueue_ = New<TActionQueue>("Control");

    BIND(&TBootstrap::DoInitialize, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrap::Run()
{
    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
    Sleep(TDuration::Max());
}

void TBootstrap::DumpSnapshot(const Stroka& fileName)
{
    BIND(&TBootstrap::DoDumpSnapshot, this, fileName)
        .AsyncVia(HydraFacade_->GetAutomatonInvoker())
        .Run()
        .Get()
        .ThrowOnError();
    _exit(0);
}

TPeerId TBootstrap::ComputePeerId(TCellConfigPtr config, const Stroka& localAddress)
{
    for (TPeerId id = 0; id < config->Addresses.size(); ++id) {
        const auto& peerAddress = config->Addresses[id];
        if (peerAddress && to_lower(*peerAddress) == to_lower(localAddress)) {
            return id;
        }
    }
    return InvalidPeerId;
}

void TBootstrap::DoInitialize()
{
    Config_->PrimaryMaster->ValidateAllPeersPresent();
    for (auto cellConfig : Config_->SecondaryMasters) {
        cellConfig->ValidateAllPeersPresent();
    }

    auto localAdddress = BuildServiceAddress(
        TAddressResolver::Get()->GetLocalHostName(),
        Config_->RpcPort);

    TCellConfigPtr localCellConfig;
    TPeerId localPeerId;

    auto primaryId = ComputePeerId(Config_->PrimaryMaster, localAdddress);
    if (primaryId == InvalidPeerId) {
        for (auto cellConfig : Config_->SecondaryMasters) {
            auto secondaryId = ComputePeerId(cellConfig, localAdddress);
            if (secondaryId != InvalidPeerId) {
                SecondaryMaster_ = true;
                localCellConfig = cellConfig;
                localPeerId = secondaryId;
                break;
            }
        }
    } else {
        PrimaryMaster_ = true;
        localCellConfig = Config_->PrimaryMaster;
        localPeerId = primaryId;
    }

    if (!PrimaryMaster_ && !SecondaryMaster_) {
        THROW_ERROR_EXCEPTION("Local address %v is not recognized as a valid master address",
            localAdddress);
    }

    Multicell_ = !Config_->SecondaryMasters.empty();

    CellId_ = localCellConfig->CellId;
    CellTag_ = CellTagFromId(CellId_);

    PrimaryCellId_ = Config_->PrimaryMaster->CellId;
    PrimaryCellTag_ = CellTagFromId(PrimaryCellId_);

    for (const auto& cellConfig : Config_->SecondaryMasters) {
        SecondaryCellTags_.push_back(CellTagFromId(cellConfig->CellId));
    }

    if (PrimaryMaster_) {
        LOG_INFO("Running as primary master (CellId: %v, CellTag: %v, SecondaryCellTags: [%v], PeerId: %v)",
            CellId_,
            CellTag_,
            JoinToString(SecondaryCellTags_),
            localPeerId);
    } else if (SecondaryMaster_) {
        LOG_INFO("Running as secondary master (CellId: %v, CellTag: %v, PrimaryCellTag: %v, PeerId: %v)",
            CellId_,
            CellTag_,
            PrimaryCellTag_,
            localPeerId);
    }

    CellDirectory_ = New<TCellDirectory>(
        Config_->CellDirectory,
        GetBusChannelFactory(),
        NNodeTrackerClient::InterconnectNetworkName);

    YCHECK(CellDirectory_->ReconfigureCell(Config_->PrimaryMaster));
    for (const auto& cellConfig : Config_->SecondaryMasters) {
        YCHECK(CellDirectory_->ReconfigureCell(cellConfig));
    }

    HttpServer_.reset(new NHttp::TServer(Config_->MonitoringPort));

    auto busServerConfig = TTcpBusServerConfig::CreateTcp(Config_->RpcPort);
    auto busServer = CreateTcpBusServer(busServerConfig);

    RpcServer_ = CreateBusServer(busServer);

    CellManager_ = New<TCellManager>(
        localCellConfig,
        GetBusChannelFactory(),
        localPeerId);

    ChangelogStore_ = CreateLocalChangelogStore(
        "ChangelogFlush",
        Config_->Changelogs);

    auto fileSnapshotStore = New<TFileSnapshotStore>(
        Config_->Snapshots);

    SnapshotStore_ = CreateLocalSnapshotStore(
        Config_->HydraManager,
        CellManager_,
        fileSnapshotStore);

    HydraFacade_ = New<THydraFacade>(Config_, this);

    MulticellManager_ = New<TMulticellManager>(Config_->MulticellManager, this);

    WorldInitializer_ = New<TWorldInitializer>(Config_, this);

    if (SecondaryMaster_) {
        CellDirectorySynchronizer_ = New<TCellDirectorySynchronizer>(
            Config_->CellDirectorySynchronizer,
            CellDirectory_,
            PrimaryCellId_);
    }

    HiveManager_ = New<THiveManager>(
        Config_->HiveManager,
        CellDirectory_,
        CellId_,
        HydraFacade_->GetAutomatonInvoker(),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton());

    // NB: This is exactly the order in which parts get registered and there are some
    // dependencies in Clear methods.
    ObjectManager_ = New<TObjectManager>(Config_->ObjectManager, this);

    SecurityManager_ = New<TSecurityManager>(Config_->SecurityManager, this);

    TransactionManager_ = New<TTransactionManager>(Config_->TransactionManager, this);

    NodeTracker_ = New<TNodeTracker>(Config_->NodeTracker, this);

    CypressManager_ = New<TCypressManager>(Config_->CypressManager, this);

    ChunkManager_ = New<TChunkManager>(Config_->ChunkManager, this);

    TabletManager_ = New<TTabletManager>(Config_->TabletManager, this);

    auto timestampManager = New<TTimestampManager>(
        Config_->TimestampManager,
        HydraFacade_->GetAutomatonInvoker(),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton());

    auto timestampProvider = CreateRemoteTimestampProvider(
        Config_->TimestampProvider,
        GetBusChannelFactory());

    TransactionSupervisor_ = New<TTransactionSupervisor>(
        Config_->TransactionSupervisor,
        HydraFacade_->GetAutomatonInvoker(),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton(),
        HydraFacade_->GetResponseKeeper(),
        HiveManager_,
        TransactionManager_,
        timestampProvider);

    fileSnapshotStore->Initialize();
    ObjectManager_->Initialize();
    SecurityManager_->Initialize();
    TransactionManager_->Initialize();
    NodeTracker_->Initialize();
    CypressManager_->Initialize();
    ChunkManager_->Initialize();
    TabletManager_->Initialize();
    if (CellDirectorySynchronizer_) {
        CellDirectorySynchronizer_->Start();
    }

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        TRefCountedTracker::Get()->GetMonitoringProducer());
    MonitoringManager_->Register(
        "/hydra",
        HydraFacade_->GetHydraManager()->GetMonitoringProducer());

    auto orchidFactory = GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        CreateVirtualNode(CreateYsonFileService(ConfigFileName_)));

    SetBuildAttributes(orchidRoot, "master");

    RpcServer_->RegisterService(CreateOrchidService(orchidRoot, GetControlInvoker())); // null realm
    RpcServer_->RegisterService(timestampManager->GetRpcService()); // null realm
    RpcServer_->RegisterService(HiveManager_->GetRpcService()); // cell realm
    RpcServer_->RegisterService(TransactionSupervisor_->GetRpcService()); // cell realm
    RpcServer_->RegisterService(New<TLocalSnapshotService>(CellId_, fileSnapshotStore)); // cell realm
    RpcServer_->RegisterService(CreateNodeTrackerService(Config_->NodeTracker, this)); // master hydra service
    RpcServer_->RegisterService(CreateObjectService(this)); // master hydra service
    RpcServer_->RegisterService(CreateJobTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateChunkService(this)); // master hydra service

    CypressManager_->RegisterHandler(CreateSysNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostVitalChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::UnderreplicatedChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::OverreplicatedChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::DataMissingChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ParityMissingChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::QuorumMissingChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::UnsafelyPlacedChunkMap));
    CypressManager_->RegisterHandler(CreateChunkListMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTransactionMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTopmostTransactionMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateLockMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateOrchidTypeHandler(this));
    CypressManager_->RegisterHandler(CreateClusterNodeNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateClusterNodeMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateRackMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateFileTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTableTypeHandler(this));
    CypressManager_->RegisterHandler(CreateJournalTypeHandler(this));
    CypressManager_->RegisterHandler(CreateAccountMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateUserMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateGroupMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletMapTypeHandler(this));

    HttpServer_->Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(orchidRoot->Via(GetControlInvoker())));

    RpcServer_->Configure(Config_->RpcServer);
}

void TBootstrap::DoRun()
{
    HydraFacade_->Initialize();

    MonitoringManager_->Start();

    LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Start();
}

void TBootstrap::DoDumpSnapshot(const Stroka& fileName)
{
    auto reader = CreateFileSnapshotReader(fileName, InvalidSegmentId, false);
    HydraFacade_->DumpSnapshot(reader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
