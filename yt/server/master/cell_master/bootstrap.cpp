#include "bootstrap.h"
#include "private.h"
#include "annotation_setter.h"
#include "config.h"
#include "config_manager.h"
#include "epoch_history_manager.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "multicell_manager.h"

#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/chunk_service.h>
#include <yt/server/master/chunk_server/cypress_integration.h>
#include <yt/server/master/chunk_server/job_tracker_service.h>

#include <yt/server/master/cypress_server/cypress_integration.h>
#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/portal_manager.h>

#include <yt/server/master/file_server/file_node_type_handler.h>

#include <yt/server/lib/hive/hive_manager.h>
#include <yt/server/lib/hive/transaction_manager.h>
#include <yt/server/lib/hive/transaction_supervisor.h>
#include <yt/server/lib/hive/transaction_participant_provider.h>

#include <yt/server/master/hive/cell_directory_synchronizer.h>

#include <yt/server/lib/hydra/changelog.h>
#include <yt/server/lib/hydra/file_snapshot_store.h>
#include <yt/server/lib/hydra/local_changelog_store.h>
#include <yt/server/lib/hydra/local_snapshot_service.h>
#include <yt/server/lib/hydra/local_snapshot_store.h>
#include <yt/server/lib/hydra/snapshot.h>

#include <yt/server/lib/discovery_server/config.h>
#include <yt/server/lib/discovery_server/discovery_service.h>

#include <yt/server/master/journal_server/journal_manager.h>
#include <yt/server/master/journal_server/journal_node.h>
#include <yt/server/master/journal_server/journal_node_type_handler.h>

#include <yt/server/master/cell_server/tamed_cell_manager.h>
#include <yt/server/master/cell_server/cell_hydra_janitor.h>

#include <yt/server/master/node_tracker_server/cypress_integration.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/server/master/node_tracker_server/node_tracker_service.h>

#include <yt/server/master/object_server/cypress_integration.h>
#include <yt/server/master/object_server/object_manager.h>
#include <yt/server/master/object_server/object_service.h>
#include <yt/server/master/object_server/request_profiling_manager.h>
#include <yt/server/master/object_server/sys_node_type_handler.h>

#include <yt/server/master/scheduler_pool_server/cypress_integration.h>
#include <yt/server/master/scheduler_pool_server/scheduler_pool.h>
#include <yt/server/master/scheduler_pool_server/scheduler_pool_manager.h>

#include <yt/server/master/orchid/cypress_integration.h>

#include <yt/server/master/security_server/cypress_integration.h>
#include <yt/server/master/security_server/security_manager.h>

#include <yt/server/master/table_server/table_node_type_handler.h>
#include <yt/server/master/table_server/replicated_table_node_type_handler.h>

#include <yt/server/master/tablet_server/cypress_integration.h>
#include <yt/server/master/tablet_server/tablet_manager.h>
#include <yt/server/master/tablet_server/tablet_cell_map_type_handler.h>
#include <yt/server/master/tablet_server/replicated_table_tracker.h>

#include <yt/server/master/transaction_server/cypress_integration.h>
#include <yt/server/master/transaction_server/transaction_manager.h>
#include <yt/server/master/transaction_server/transaction_service.h>

#include <yt/server/lib/election/election_manager.h>

#include <yt/server/lib/admin/admin_service.h>

#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/server/lib/timestamp_server/timestamp_manager.h>

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

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/local_address.h>

#include <yt/core/http/server.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/ytalloc/statistics_producer.h>

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

using namespace NAdmin;
using namespace NApi;
using namespace NBus;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NElection;
using namespace NFileServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NJournalServer;
using namespace NJournalServer;
using namespace NMonitoring;
using namespace NNet;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSchedulerPoolServer;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTabletServer;
using namespace NTimestampServer;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NCellServer;
using namespace NDiscoveryServer;

using NTransactionServer::TTransactionManager;
using NTransactionServer::TTransactionManagerPtr;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");
static const NProfiling::TProfiler BootstrapProfiler("");
static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TCellMasterConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }
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

const TPortalManagerPtr& TBootstrap::GetPortalManager() const
{
    return PortalManager_;
}

const THydraFacadePtr& TBootstrap::GetHydraFacade() const
{
    return HydraFacade_;
}

const TEpochHistoryManagerPtr& TBootstrap::GetEpochHistoryManager() const
{
    return EpochHistoryManager_;
}

const TWorldInitializerPtr& TBootstrap::GetWorldInitializer() const
{
    return WorldInitializer_;
}

const TObjectManagerPtr& TBootstrap::GetObjectManager() const
{
    return ObjectManager_;
}

const TRequestProfilingManagerPtr& TBootstrap::GetRequestProfilingManager() const
{
    return RequestProfilingManager_;
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

const TSchedulerPoolManagerPtr& TBootstrap::GetSchedulerPoolManager() const
{
    return SchedulerPoolManager_;
}

const TTamedCellManagerPtr& TBootstrap::GetTamedCellManager() const
{
    return TamedCellManager_;
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

const IInvokerPtr& TBootstrap::GetProfilerInvoker() const
{
    return ProfilerQueue_->GetInvoker();
}

const INodeChannelFactoryPtr& TBootstrap::GetNodeChannelFactory() const
{
    return NodeChannelFactory_;
}

void TBootstrap::Initialize()
{
    srand(time(nullptr));

    ControlQueue_ = New<TActionQueue>("Control");
    ProfilerQueue_ = New<TActionQueue>("Profiler");

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

void TBootstrap::TryLoadSnapshot(
    const TString& fileName,
    bool dump,
    bool enableTotalWriteCountReport,
    const TString& dumpConfigString)
{
    TSerializationDumperConfigPtr dumpConfig;
    ValidateLoadSnapshotParameters(dump, enableTotalWriteCountReport, dumpConfigString, &dumpConfig);

    BIND(&TBootstrap::DoLoadSnapshot, this, fileName, dump, enableTotalWriteCountReport, dumpConfig)
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
    TCellTagList participantCellTags;
    participantCellTags.push_back(PrimaryCellTag_);
    participantCellTags.insert(participantCellTags.end(), SecondaryCellTags_.begin(), SecondaryCellTags_.end());
    return participantCellTags;
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

    return NNative::CreateConnection(config);
}

void TBootstrap::OnProfiling()
{
    try {
        auto snapshotsStorageDiskSpaceStatistics = NFS::GetDiskSpaceStatistics(Config_->Snapshots->Path);
        Profiler_.Enqueue("/snapshots/free_space",
            snapshotsStorageDiskSpaceStatistics.FreeSpace,
            EMetricType::Gauge);
        Profiler_.Enqueue("/snapshots/available_space",
            snapshotsStorageDiskSpaceStatistics.AvailableSpace,
            EMetricType::Gauge);
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Failed to profile snapshots storage disk space");
    }

    try {
        auto changelogsStorageDiskSpaceStatistics = NFS::GetDiskSpaceStatistics(Config_->Changelogs->Path);
        Profiler_.Enqueue("/changelogs/free_space",
            changelogsStorageDiskSpaceStatistics.FreeSpace,
            EMetricType::Gauge);
        Profiler_.Enqueue("/changelogs/available_space",
            changelogsStorageDiskSpaceStatistics.AvailableSpace,
            EMetricType::Gauge);
    } catch (const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Failed to profile changelogs storage disk space");
    }
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

    YT_VERIFY(CellDirectory_->ReconfigureCell(Config_->PrimaryMaster));
    for (const auto& cellConfig : Config_->SecondaryMasters) {
        YT_VERIFY(CellDirectory_->ReconfigureCell(cellConfig));
    }

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    Config_->MonitoringServer->ServerName = "monitoring";

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

    EpochHistoryManager_ = New<TEpochHistoryManager>(this);

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
    ObjectManager_ = New<TObjectManager>(this);

    RequestProfilingManager_ = New<TRequestProfilingManager>();

    SecurityManager_ = New<TSecurityManager>(this);

    TransactionManager_ = New<TTransactionManager>(this);

    NodeTracker_ = New<TNodeTracker>(Config_->NodeTracker, this);

    CypressManager_ = New<TCypressManager>(this);

    PortalManager_ =  New<TPortalManager>(this);

    ChunkManager_ = New<TChunkManager>(Config_->ChunkManager, this);

    JournalManager_ = New<NJournalServer::TJournalManager>(this);

    TamedCellManager_ = New<TTamedCellManager>(this);

    CellHydraJanitor_ = New<TCellHydraJanitor>(this);

    TabletManager_ = New<TTabletManager>(Config_->TabletManager, this);

    ReplicatedTableTracker_ = New<TReplicatedTableTracker>(Config_->ReplicatedTableTracker, this);

    SchedulerPoolManager_ = New<TSchedulerPoolManager>(this);

    if (Config_->EnableTimestampManager && MulticellManager_->IsPrimaryMaster()) {
        TimestampManager_ = New<TTimestampManager>(
            Config_->TimestampManager,
            HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TimestampManager),
            HydraFacade_->GetHydraManager(),
            HydraFacade_->GetAutomaton());
    }

    TimestampProvider_ = CreateRemoteTimestampProvider(
        Config_->TimestampProvider,
        CreateTimestampProviderChannel(Config_->TimestampProvider, channelFactory));

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
    TamedCellManager_->Initialize();
    CellHydraJanitor_->Initialize();
    TabletManager_->Initialize();
    MulticellManager_->Initialize();
    SchedulerPoolManager_->Initialize();

    CellDirectorySynchronizer_ = New<NHiveServer::TCellDirectorySynchronizer>(
        Config_->CellDirectorySynchronizer,
        CellDirectory_,
        TamedCellManager_,
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::CellDirectorySynchronizer));
    CellDirectorySynchronizer_->Start();

    DiscoveryQueue_ = New<TActionQueue>("Discovery");
    auto discoveryServerConfig = New<TDiscoveryServerConfig>();
    discoveryServerConfig->ServerAddresses.reserve(localCellConfig->Peers.size());
    for (const auto& peer : localCellConfig->Peers) {
        if (peer.Address) {
            discoveryServerConfig->ServerAddresses.push_back(*peer.Address);
        }
    }
    DiscoveryServer_ = New<TDiscoveryServer>(
        RpcServer_,
        localAddress,
        discoveryServerConfig,
        channelFactory,
        DiscoveryQueue_->GetInvoker(),
        DiscoveryQueue_->GetInvoker());
    DiscoveryServer_->Initialize();

    if (TimestampManager_) {
        RpcServer_->RegisterService(TimestampManager_->GetRpcService()); // null realm
    }
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
    CypressManager_->RegisterHandler(CreateChunkViewMapTypeHandler(this));
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
    CypressManager_->RegisterHandler(CreateNetworkProjectMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreatePoolTreeMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletCellBundleMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletActionMapTypeHandler(this));

    CypressManager_->RegisterHandler(CreateEstimatedCreationTimeMapTypeHandler(this));

    RpcServer_->Configure(Config_->RpcServer);

    AnnotationSetter_ = New<TAnnotationSetter>(this);

    Profiler_ = BootstrapProfiler;

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        GetProfilerInvoker(),
        BIND(&TBootstrap::OnProfiling, this),
        ProfilingPeriod);
    ProfilingExecutor_->Start();
}

void TBootstrap::DoRun()
{
    HydraFacade_->Initialize();

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &orchidRoot);
    MonitoringManager_->Register(
        "/hydra",
        HydraFacade_->GetHydraManager()->GetMonitoringProducer());
    MonitoringManager_->Register(
        "/election",
        HydraFacade_->GetElectionManager()->GetMonitoringProducer());

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
    SetNodeByYPath(
        orchidRoot,
        "/discovery_server",
        CreateVirtualNode(DiscoveryServer_->GetYPathService()));

    SetBuildAttributes(orchidRoot, "master");

    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->RegisterService(CreateOrchidService(orchidRoot, GetControlInvoker()));
    RpcServer_->Start();

    AnnotationSetter_->Start();
}

void TBootstrap::DoLoadSnapshot(
    const TString& fileName,
    bool dump,
    bool enableTotalWriteCountReport,
    const TSerializationDumperConfigPtr& dumpConfig)
{
    auto reader = CreateFileSnapshotReader(fileName, InvalidSegmentId, false);
    HydraFacade_->LoadSnapshot(reader, dump, enableTotalWriteCountReport, dumpConfig);
}

void TBootstrap::ValidateLoadSnapshotParameters(
    bool dump,
    bool enableTotalWriteCountReport,
    const TString& dumpConfigString,
    TSerializationDumperConfigPtr* dumpConfig)
{
    if (dump && enableTotalWriteCountReport) {
        THROW_ERROR_EXCEPTION("'EnableTotalWriteCountReport' can be specified only for snapshot validation");
    }

    if (dumpConfigString) {
        if (!dump) {
            THROW_ERROR_EXCEPTION("'DumpConfig' can be specified only for snapshot dumping");
        }
        *dumpConfig = ConvertTo<TSerializationDumperConfigPtr>(NYson::TYsonString(dumpConfigString));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
