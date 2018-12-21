#include "bootstrap.h"
#include "private.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "multicell_manager.h"

#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/chunk_service.h>
#include <yt/server/chunk_server/cypress_integration.h>
#include <yt/server/chunk_server/job_tracker_service.h>

#include <yt/server/cypress_server/cypress_integration.h>
#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/file_server/file_node.h>

#include <yt/server/hive/hive_manager.h>
#include <yt/server/hive/transaction_manager.h>
#include <yt/server/hive/transaction_supervisor.h>
#include <yt/server/hive/transaction_participant_provider.h>
#include <yt/server/hive/cell_directory_synchronizer.h>

#include <yt/server/hydra/changelog.h>
#include <yt/server/hydra/file_snapshot_store.h>
#include <yt/server/hydra/local_changelog_store.h>
#include <yt/server/hydra/local_snapshot_service.h>
#include <yt/server/hydra/local_snapshot_store.h>
#include <yt/server/hydra/snapshot.h>

#include <yt/server/journal_server/journal_node.h>
#include <yt/server/journal_server/journal_manager.h>

#include <yt/server/node_tracker_server/cypress_integration.h>
#include <yt/server/node_tracker_server/node_tracker.h>
#include <yt/server/node_tracker_server/node_tracker_service.h>

#include <yt/server/object_server/object_manager.h>
#include <yt/server/object_server/object_service.h>
#include <yt/server/object_server/sys_node_type_handler.h>

#include <yt/server/orchid/cypress_integration.h>

#include <yt/server/security_server/cypress_integration.h>
#include <yt/server/security_server/security_manager.h>

#include <yt/server/table_server/table_node_type_handler.h>
#include <yt/server/table_server/replicated_table_node_type_handler.h>

#include <yt/server/tablet_server/cypress_integration.h>
#include <yt/server/tablet_server/tablet_manager.h>
#include <yt/server/tablet_server/tablet_cell_map_type_handler.h>
#include <yt/server/tablet_server/replicated_table_tracker.h>

#include <yt/server/transaction_server/cypress_integration.h>
#include <yt/server/transaction_server/timestamp_manager.h>
#include <yt/server/transaction_server/transaction_manager.h>
#include <yt/server/transaction_server/transaction_service.h>

#include <yt/server/election/election_manager.h>

#include <yt/server/admin_server/admin_service.h>

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/election/cell_manager.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/client/transaction_client/remote_timestamp_provider.h>
#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/core_dump/core_dumper.h>

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/net/local_address.h>

#include <yt/core/http/server.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/local_channel.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_service.h>

namespace NYT::NCellMaster {

using namespace NApi;
using namespace NAdmin;
using namespace NBus;
using namespace NRpc;
using namespace NNet;
using namespace NYTree;
using namespace NElection;
using namespace NHydra;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NTransactionClient;
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

using NTransactionServer::TTransactionManager;
using NTransactionServer::TTransactionManagerPtr;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TCellMasterConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    WarnForUnrecognizedOptions(Logger, Config_);
}

TBootstrap::~TBootstrap() = default;

const TCellMasterConfigPtr& TBootstrap::GetConfig() const
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

TCellId TBootstrap::GetCellId() const
{
    return CellId_;
}

TCellId TBootstrap::GetCellId(TCellTag cellTag) const
{
    return cellTag == PrimaryMasterCellTag
        ? PrimaryCellId_
        : ReplaceCellTagInId(PrimaryCellId_, cellTag);
}

TCellTag TBootstrap::GetCellTag() const
{
    return CellTag_;
}

TCellId TBootstrap::GetPrimaryCellId() const
{
    return PrimaryCellId_;
}

TCellTag TBootstrap::GetPrimaryCellTag() const
{
    return PrimaryCellTag_;
}

const TCellTagList& TBootstrap::GetSecondaryCellTags() const
{
    return SecondaryCellTags_;
}

const TConfigManagerPtr& TBootstrap::GetConfigManager() const
{
    return ConfigManager_;
}

const TMulticellManagerPtr& TBootstrap::GetMulticellManager() const
{
    return MulticellManager_;
}

const IServerPtr& TBootstrap::GetRpcServer() const
{
    return RpcServer_;
}

const IChannelPtr& TBootstrap::GetLocalRpcChannel() const
{
    return LocalRpcChannel_;
}

const NNative::IConnectionPtr& TBootstrap::GetClusterConnection() const
{
    return ClusterConnection_;
}

const TCellManagerPtr& TBootstrap::GetCellManager() const
{
    return CellManager_;
}

const IChangelogStoreFactoryPtr& TBootstrap::GetChangelogStoreFactory() const
{
    return ChangelogStoreFactory_;
}

const ISnapshotStorePtr& TBootstrap::GetSnapshotStore() const
{
    return SnapshotStore_;
}

const TNodeTrackerPtr& TBootstrap::GetNodeTracker() const
{
    return NodeTracker_;
}

const TTransactionManagerPtr& TBootstrap::GetTransactionManager() const
{
    return TransactionManager_;
}

const TTransactionSupervisorPtr& TBootstrap::GetTransactionSupervisor() const
{
    return TransactionSupervisor_;
}

const ITimestampProviderPtr& TBootstrap::GetTimestampProvider() const
{
    return TimestampProvider_;
}

const TCypressManagerPtr& TBootstrap::GetCypressManager() const
{
    return CypressManager_;
}

const THydraFacadePtr& TBootstrap::GetHydraFacade() const
{
    return HydraFacade_;
}

const TWorldInitializerPtr& TBootstrap::GetWorldInitializer() const
{
    return WorldInitializer_;
}

const TObjectManagerPtr& TBootstrap::GetObjectManager() const
{
    return ObjectManager_;
}

const TChunkManagerPtr& TBootstrap::GetChunkManager() const
{
    return ChunkManager_;
}

const TJournalManagerPtr& TBootstrap::GetJournalManager() const
{
    return JournalManager_;
}

const TSecurityManagerPtr& TBootstrap::GetSecurityManager() const
{
    return SecurityManager_;
}

const TTabletManagerPtr& TBootstrap::GetTabletManager() const
{
    return TabletManager_;
}

const THiveManagerPtr& TBootstrap::GetHiveManager() const
{
    return HiveManager_;
}

const TCellDirectoryPtr& TBootstrap::GetCellDirectory() const
{
    return CellDirectory_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const INodeChannelFactoryPtr& TBootstrap::GetNodeChannelFactory() const
{
    return NodeChannelFactory_;
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

void TBootstrap::TryLoadSnapshot(const TString& fileName, bool dump)
{
    BIND(&TBootstrap::DoLoadSnapshot, this, fileName, dump)
        .AsyncVia(HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

TPeerId TBootstrap::ComputePeerId(TCellConfigPtr config, const TString& localAddress)
{
    for (TPeerId id = 0; id < config->Peers.size(); ++id) {
        const auto& peerAddress = config->Peers[id].Address;
        if (peerAddress && to_lower(*peerAddress) == to_lower(localAddress)) {
            return id;
        }
    }
    return InvalidPeerId;
}

TCellTagList TBootstrap::GetKnownParticipantCellTags() const
{
    TCellTagList participnatCellTags{PrimaryCellTag_};
    if (IsPrimaryMaster()) {
        participnatCellTags.insert(participnatCellTags.end(), SecondaryCellTags_.begin(), SecondaryCellTags_.end());
    } else {
        participnatCellTags.push_back(CellTag_);
    }
    return participnatCellTags;
}

NNative::IConnectionPtr TBootstrap::CreateClusterConnection() const
{
    auto cloneMasterConfig = [] (const auto& cellConfig) {
        auto result = New<NNative::TMasterConnectionConfig>();
        result->CellId = cellConfig->CellId;
        result->Addresses.reserve(cellConfig->Peers.size());
        for (const auto& peer : cellConfig->Peers) {
            if (peer.Address) {
                result->Addresses.push_back(*peer.Address);
            }
        }
        return result;
    };

    auto config = New<NNative::TConnectionConfig>();
    config->Networks = Config_->Networks;
    config->PrimaryMaster = cloneMasterConfig(Config_->PrimaryMaster);
    config->SecondaryMasters.reserve(Config_->SecondaryMasters.size());
    for (const auto& secondaryMaster : Config_->SecondaryMasters) {
        config->SecondaryMasters.push_back(cloneMasterConfig(secondaryMaster));
    }
    config->TimestampProvider = Config_->TimestampProvider;
    config->UseTabletService = true;

    return NNative::CreateConnection(config);
}

void TBootstrap::DoInitialize()
{
    Config_->PrimaryMaster->ValidateAllPeersPresent();
    for (auto cellConfig : Config_->SecondaryMasters) {
        cellConfig->ValidateAllPeersPresent();
    }

    auto localAddress = BuildServiceAddress(GetLocalHostName(), Config_->RpcPort);

    TCellConfigPtr localCellConfig;
    TPeerId localPeerId;

    auto primaryId = ComputePeerId(Config_->PrimaryMaster, localAddress);
    if (primaryId == InvalidPeerId) {
        for (auto cellConfig : Config_->SecondaryMasters) {
            auto secondaryId = ComputePeerId(cellConfig, localAddress);
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
            localAddress);
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
        YT_LOG_INFO("Running as primary master (CellId: %v, CellTag: %v, SecondaryCellTags: %v, PeerId: %v)",
            CellId_,
            CellTag_,
            SecondaryCellTags_,
            localPeerId);
    } else if (SecondaryMaster_) {
        YT_LOG_INFO("Running as secondary master (CellId: %v, CellTag: %v, PrimaryCellTag: %v, PeerId: %v)",
            CellId_,
            CellTag_,
            PrimaryCellTag_,
            localPeerId);
    }

    auto channelFactory = CreateCachingChannelFactory(NRpc::NBus::CreateBusChannelFactory(Config_->BusClient));

    const auto& networks = Config_->Networks;

    NodeChannelFactory_ = CreateNodeChannelFactory(channelFactory, networks);

    CellDirectory_ = New<TCellDirectory>(
        Config_->CellDirectory,
        channelFactory,
        networks,
        Logger);

    YCHECK(CellDirectory_->ReconfigureCell(Config_->PrimaryMaster));
    for (const auto& cellConfig : Config_->SecondaryMasters) {
        YCHECK(CellDirectory_->ReconfigureCell(cellConfig));
    }

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    auto busServer = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(busServer);

    LocalRpcChannel_ = CreateRealmChannel(
        CreateLocalChannel(RpcServer_),
        CellId_);

    ClusterConnection_ = CreateClusterConnection();

    CellManager_ = New<TCellManager>(
        localCellConfig,
        channelFactory,
        localPeerId);

    ChangelogStoreFactory_ = CreateLocalChangelogStoreFactory(
        Config_->Changelogs,
        "ChangelogFlush",
        NProfiling::TProfiler("/changelogs"));

    auto fileSnapshotStore = New<TFileSnapshotStore>(
        Config_->Snapshots);

    SnapshotStore_ = CreateLocalSnapshotStore(
        Config_->HydraManager,
        CellManager_,
        fileSnapshotStore);

    HydraFacade_ = New<THydraFacade>(Config_, this);

    ConfigManager_ = New<TConfigManager>(this);

    MulticellManager_ = New<TMulticellManager>(Config_->MulticellManager, this);

    WorldInitializer_ = New<TWorldInitializer>(Config_, this);

    HiveManager_ = New<THiveManager>(
        Config_->HiveManager,
        CellDirectory_,
        CellId_,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::HiveManager),
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

    JournalManager_ = New<NJournalServer::TJournalManager>(Config_->JournalManager, this);

    TabletManager_ = New<TTabletManager>(Config_->TabletManager, this);

    ReplicatedTableTracker_ = New<TReplicatedTableTracker>(Config_->ReplicatedTableTracker, this);

    auto timestampManager = New<TTimestampManager>(
        Config_->TimestampManager,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TimestampManager),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton());

    TimestampProvider_ = CreateRemoteTimestampProvider(
        Config_->TimestampProvider,
        channelFactory);

    // Initialize periodic latest timestamp update.
    TimestampProvider_->GetLatestTimestamp();

    TransactionSupervisor_ = New<TTransactionSupervisor>(
        Config_->TransactionSupervisor,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionSupervisor),
        HydraFacade_->GetTransactionTrackerInvoker(),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton(),
        HydraFacade_->GetResponseKeeper(),
        TransactionManager_,
        SecurityManager_,
        CellId_,
        TimestampProvider_,
        std::vector<ITransactionParticipantProviderPtr>{
            CreateTransactionParticipantProvider(
                CellDirectory_,
                TimestampProvider_,
                GetKnownParticipantCellTags())
        });

    fileSnapshotStore->Initialize();
    ConfigManager_->Initialize();
    ObjectManager_->Initialize();
    SecurityManager_->Initialize();
    TransactionManager_->Initialize();
    NodeTracker_->Initialize();
    CypressManager_->Initialize();
    ChunkManager_->Initialize();
    TabletManager_->Initialize();

    CellDirectorySynchronizer_ = New<NHiveServer::TCellDirectorySynchronizer>(
        Config_->CellDirectorySynchronizer,
        CellDirectory_,
        TabletManager_,
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic));
    CellDirectorySynchronizer_->Start();

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());
    MonitoringManager_->Register(
        "/hydra",
        HydraFacade_->GetHydraManager()->GetMonitoringProducer());
    MonitoringManager_->Register(
        "/election",
        HydraFacade_->GetElectionManager()->GetMonitoringProducer());

    auto orchidRoot = GetEphemeralNodeFactory(true)->CreateMap();
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
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/chunk_manager",
        CreateVirtualNode(ChunkManager_->GetOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/hive",
        CreateVirtualNode(HiveManager_->GetOrchidService()));

    OrchidHttpHandler_ = NMonitoring::GetOrchidYPathHttpHandler(orchidRoot);

    SetBuildAttributes(orchidRoot, "master");

    RpcServer_->RegisterService(CreateOrchidService(orchidRoot, GetControlInvoker())); // null realm
    RpcServer_->RegisterService(timestampManager->GetRpcService()); // null realm
    RpcServer_->RegisterService(HiveManager_->GetRpcService()); // cell realm
    for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
        RpcServer_->RegisterService(service); // cell realm
    }
    RpcServer_->RegisterService(CreateLocalSnapshotService(CellId_, fileSnapshotStore)); // cell realm
    RpcServer_->RegisterService(CreateNodeTrackerService(Config_->NodeTracker, this)); // master hydra service
    RpcServer_->RegisterService(CreateObjectService(Config_->ObjectService, this)); // master hydra service
    RpcServer_->RegisterService(CreateJobTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateChunkService(this)); // master hydra service
    RpcServer_->RegisterService(CreateAdminService(GetControlInvoker(), CoreDumper_));
    RpcServer_->RegisterService(CreateTransactionService(this)); // master hydra service

    CypressManager_->RegisterHandler(CreateSysNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::LostVitalChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::PrecariousChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::PrecariousVitalChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::UnderreplicatedChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::OverreplicatedChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::DataMissingChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ParityMissingChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::QuorumMissingChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::UnsafelyPlacedChunkMap));
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::ForeignChunkMap));
    CypressManager_->RegisterHandler(CreateChunkListMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateMediumMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTransactionMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTopmostTransactionMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateLockMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateOrchidTypeHandler(this));
    CypressManager_->RegisterHandler(CreateClusterNodeNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateClusterNodeMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateRackMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateDataCenterMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateFileTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTableTypeHandler(this));
    CypressManager_->RegisterHandler(CreateReplicatedTableTypeHandler(this));
    CypressManager_->RegisterHandler(CreateJournalTypeHandler(this));
    CypressManager_->RegisterHandler(CreateAccountMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateUserMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateGroupMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellBundleMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletActionMapTypeHandler(this));

    RpcServer_->Configure(Config_->RpcServer);
}

void TBootstrap::DoRun()
{
    HydraFacade_->Initialize();

    MonitoringManager_->Start();

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);
    HttpServer_->AddHandler("/orchid/", OrchidHttpHandler_);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Start();
}

void TBootstrap::DoLoadSnapshot(const TString& fileName, bool dump)
{
    auto reader = CreateFileSnapshotReader(fileName, InvalidSegmentId, false);
    HydraFacade_->LoadSnapshot(reader, dump);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
