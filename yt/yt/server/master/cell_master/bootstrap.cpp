#include "bootstrap.h"
#include "private.h"
#include "alert_manager.h"
#include "config.h"
#include "config_manager.h"
#include "epoch_history_manager.h"
#include "hydra_facade.h"
#include "world_initializer.h"
#include "multicell_manager.h"

#include <yt/yt/server/master/chaos_server/chaos_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_service.h>
#include <yt/yt/server/master/chunk_server/data_node_tracker.h>
#include <yt/yt/server/master/chunk_server/data_node_tracker_service.h>
#include <yt/yt/server/master/chunk_server/cypress_integration.h>
#include <yt/yt/server/master/chunk_server/job_tracker_service.h>

#include <yt/yt/server/master/cypress_server/cypress_integration.h>
#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/cypress_service.h>
#include <yt/yt/server/master/cypress_server/portal_manager.h>

#include <yt/yt/server/master/file_server/file_node_type_handler.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/transaction_manager.h>
#include <yt/yt/server/lib/hive/transaction_supervisor.h>
#include <yt/yt/server/lib/hive/transaction_participant_provider.h>

#include <yt/yt/server/master/hive/cell_directory_synchronizer.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/file_snapshot_store.h>
#include <yt/yt/server/lib/hydra/local_changelog_store.h>
#include <yt/yt/server/lib/hydra/local_snapshot_service.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra/snapshot.h>

#include <yt/yt/server/lib/discovery_server/config.h>
#include <yt/yt/server/lib/discovery_server/discovery_server.h>

#include <yt/yt/server/master/journal_server/journal_manager.h>
#include <yt/yt/server/master/journal_server/journal_node.h>
#include <yt/yt/server/master/journal_server/journal_node_type_handler.h>

#include <yt/yt/server/master/cell_server/cell_hydra_janitor.h>
#include <yt/yt/server/master/cell_server/cell_hydra_janitor.h>
#include <yt/yt/server/master/cell_server/cell_map_type_handler.h>
#include <yt/yt/server/master/cell_server/cellar_node_tracker.h>
#include <yt/yt/server/master/cell_server/cellar_node_tracker_service.h>
#include <yt/yt/server/master/cell_server/cypress_integration.h>
#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/node_tracker_server/cypress_integration.h>
#include <yt/yt/server/master/node_tracker_server/exec_node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/exec_node_tracker_service.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker_service.h>

#include <yt/yt/server/master/object_server/cypress_integration.h>
#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/object_service.h>
#include <yt/yt/server/master/object_server/request_profiling_manager.h>
#include <yt/yt/server/master/object_server/sys_node_type_handler.h>
#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/server/master/scheduler_pool_server/cypress_integration.h>
#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool.h>
#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool_manager.h>

#include <yt/yt/server/master/orchid/cypress_integration.h>

#include <yt/yt/server/master/security_server/cypress_integration.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/table_server/cypress_integration.h>
#include <yt/yt/server/master/table_server/table_manager.h>
#include <yt/yt/server/master/table_server/table_node_type_handler.h>
#include <yt/yt/server/master/table_server/replicated_table_node_type_handler.h>

#include <yt/yt/server/master/tablet_server/cypress_integration.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>
#include <yt/yt/server/master/tablet_server/replicated_table_tracker.h>
#include <yt/yt/server/master/tablet_server/tablet_node_tracker.h>
#include <yt/yt/server/master/tablet_server/tablet_node_tracker_service.h>

#include <yt/yt/server/master/transaction_server/cypress_integration.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>
#include <yt/yt/server/master/transaction_server/transaction_service.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/server/lib/timestamp_server/timestamp_manager.h>

#include <yt/yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/program/build_attributes.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>
#include <yt/yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

#include <yt/yt/client/transaction_client/noop_timestamp_provider.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/misc/core_dumper.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/ytalloc/statistics_producer.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NCellMaster {

using namespace NAdmin;
using namespace NApi;
using namespace NBus;
using namespace NCellarClient;
using namespace NChaosServer;
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
using namespace NDistributedThrottler;

using NTransactionServer::TTransactionManager;
using NTransactionServer::TTransactionManagerPtr;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TCellMasterConfigPtr config)
    : Config_(std::move(config))
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

const IAlertManagerPtr& TBootstrap::GetAlertManager() const
{
    return AlertManager_;
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

const IDataNodeTrackerPtr& TBootstrap::GetDataNodeTracker() const
{
    return DataNodeTracker_;
}

const IExecNodeTrackerPtr& TBootstrap::GetExecNodeTracker() const
{
    return ExecNodeTracker_;
}

const ICellarNodeTrackerPtr& TBootstrap::GetCellarNodeTracker() const
{
    return CellarNodeTracker_;
}

const ITabletNodeTrackerPtr& TBootstrap::GetTabletNodeTracker() const
{
    return TabletNodeTracker_;
}

const TTransactionManagerPtr& TBootstrap::GetTransactionManager() const
{
    return TransactionManager_;
}

const ITransactionSupervisorPtr& TBootstrap::GetTransactionSupervisor() const
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

const IYsonInternRegistryPtr& TBootstrap::GetYsonInternRegistry() const
{
    return YsonInternRegistry_;
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

const TTableManagerPtr& TBootstrap::GetTableManager() const
{
    return TableManager_;
}

const ITamedCellManagerPtr& TBootstrap::GetTamedCellManager() const
{
    return TamedCellManager_;
}

const TTabletManagerPtr& TBootstrap::GetTabletManager() const
{
    return TabletManager_;
}

const IChaosManagerPtr& TBootstrap::GetChaosManager() const
{
    return ChaosManager_;
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

NDistributedThrottler::IDistributedThrottlerFactoryPtr TBootstrap::CreateDistributedThrottlerFactory(
    TDistributedThrottlerConfigPtr config,
    IInvokerPtr invoker,
    const TString& groupIdPrefix,
    NLogging::TLogger logger) const
{
    return NDistributedThrottler::CreateDistributedThrottlerFactory(
        std::move(config),
        ChannelFactory_,
        std::move(invoker),
        Format("%v/%v", groupIdPrefix, CellTag_),
        ToString(GetCellManager()->GetSelfPeerId()),
        RpcServer_,
        BuildServiceAddress(GetLocalHostName(), Config_->RpcPort),
        std::move(logger));
}

void TBootstrap::Initialize()
{
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
    for (TPeerId id = 0; id < std::ssize(config->Peers); ++id) {
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
        result->Addresses.emplace();
        for (const auto& peer : cellConfig->Peers) {
            if (peer.Address) {
                result->Addresses->push_back(*peer.Address);
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

class TDiskSpaceProfiler
    : public NProfiling::ISensorProducer
{
public:
    TDiskSpaceProfiler(const TCellMasterConfigPtr& config)
        : Config_(config)
    { }

    virtual void CollectSensors(ISensorWriter* writer) override
    {
        try {
            auto snapshotsStorageDiskSpaceStatistics = NFS::GetDiskSpaceStatistics(Config_->Snapshots->Path);
            writer->AddGauge("/snapshots/free_space", snapshotsStorageDiskSpaceStatistics.FreeSpace);
            writer->AddGauge("/snapshots/available_space", snapshotsStorageDiskSpaceStatistics.AvailableSpace);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to profile snapshots storage disk space");
        }

        try {
            auto changelogsStorageDiskSpaceStatistics = NFS::GetDiskSpaceStatistics(Config_->Changelogs->Path);
            writer->AddGauge("/changelogs/free_space", changelogsStorageDiskSpaceStatistics.FreeSpace);
            writer->AddGauge("/changelogs/available_space", changelogsStorageDiskSpaceStatistics.AvailableSpace);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to profile changelogs storage disk space");
        }
    }

private:
    TCellMasterConfigPtr Config_;
};

DEFINE_REFCOUNTED_TYPE(TDiskSpaceProfiler)

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

    ChannelFactory_ = CreateCachingChannelFactory(NRpc::NBus::CreateBusChannelFactory(Config_->BusClient));

    const auto& networks = Config_->Networks;

    NodeChannelFactory_ = CreateNodeChannelFactory(ChannelFactory_, networks);

    CellDirectory_ = New<TCellDirectory>(
        Config_->CellDirectory,
        ChannelFactory_,
        networks,
        Logger);

    YT_VERIFY(CellDirectory_->ReconfigureCell(Config_->PrimaryMaster));
    for (const auto& cellConfig : Config_->SecondaryMasters) {
        YT_VERIFY(CellDirectory_->ReconfigureCell(cellConfig));
    }

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    auto busServer = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(busServer);

    LocalRpcChannel_ = CreateRealmChannel(
        CreateLocalChannel(RpcServer_),
        CellId_);

    CellManager_ = New<TCellManager>(
        localCellConfig,
        ChannelFactory_,
        nullptr,
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

    AlertManager_ = CreateAlertManager(this);

    ConfigManager_ = New<TConfigManager>(this);
    ConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

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

    std::vector<TString> addresses;
    addresses.reserve(localCellConfig->Peers.size());
    for (const auto& peer : localCellConfig->Peers) {
        if (peer.Address) {
            addresses.push_back(*peer.Address);
        }
    }

    auto getDiscoveryServerAddresses = [&] () {
        if (Config_->DiscoveryServer && Config_->DiscoveryServer->Addresses) {
            return *Config_->DiscoveryServer->Addresses;
        }
        return addresses;
    };
    const auto& discoveryServerAddresses = getDiscoveryServerAddresses();
    Config_->SecurityManager->UserThrottler->MemberClient->ServerAddresses = discoveryServerAddresses;
    Config_->SecurityManager->UserThrottler->DiscoveryClient->ServerAddresses = discoveryServerAddresses;

    // NB: This is exactly the order in which parts get registered and there are some
    // dependencies in Clear methods.
    ObjectManager_ = New<TObjectManager>(this);

    YsonInternRegistry_ = CreateYsonInternRegistry(this);

    RequestProfilingManager_ = New<TRequestProfilingManager>();

    SecurityManager_ = New<TSecurityManager>(Config_->SecurityManager, this);

    TransactionManager_ = New<TTransactionManager>(this);

    NodeTracker_ = New<TNodeTracker>(this);

    DataNodeTracker_ = CreateDataNodeTracker(this);

    ExecNodeTracker_ = CreateExecNodeTracker(this);

    CellarNodeTracker_ = CreateCellarNodeTracker(this);

    TabletNodeTracker_ = CreateTabletNodeTracker(this);

    CypressManager_ = New<TCypressManager>(this);

    PortalManager_ =  New<TPortalManager>(this);

    ChunkManager_ = New<TChunkManager>(Config_->ChunkManager, this);

    JournalManager_ = New<NJournalServer::TJournalManager>(this);

    TamedCellManager_ = CreateTamedCellManager(this);

    CellHydraJanitor_ = New<TCellHydraJanitor>(this);

    TableManager_ = New<TTableManager>(this);

    TabletManager_ = New<TTabletManager>(this);

    ChaosManager_ = CreateChaosManager(this);

    ReplicatedTableTracker_ = New<TReplicatedTableTracker>(Config_->ReplicatedTableTracker, this);

    SchedulerPoolManager_ = New<TSchedulerPoolManager>(this);

    InitializeTimestampProvider();

    if (MulticellManager_->IsPrimaryMaster() && Config_->EnableTimestampManager) {
        TimestampManager_ = New<TTimestampManager>(
            Config_->TimestampManager,
            HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TimestampManager),
            HydraFacade_->GetHydraManager(),
            HydraFacade_->GetAutomaton());
    }

    TransactionSupervisor_ = CreateTransactionSupervisor(
        Config_->TransactionSupervisor,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TransactionSupervisor),
        HydraFacade_->GetTransactionTrackerInvoker(),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton(),
        HydraFacade_->GetResponseKeeper(),
        TransactionManager_,
        CellId_,
        TimestampProvider_,
        std::vector<ITransactionParticipantProviderPtr>{
            CreateTransactionParticipantProvider(
                CellDirectory_,
                TimestampProvider_,
                GetKnownParticipantCellTags())
        });

    fileSnapshotStore->Initialize();
    AlertManager_->Initialize();
    ObjectManager_->Initialize();
    SecurityManager_->Initialize();
    TransactionManager_->Initialize();
    NodeTracker_->Initialize();
    DataNodeTracker_->Initialize();
    ExecNodeTracker_->Initialize();
    CellarNodeTracker_->Initialize();
    TabletNodeTracker_->Initialize();
    CypressManager_->Initialize();
    ChunkManager_->Initialize();
    TamedCellManager_->Initialize();
    CellHydraJanitor_->Initialize();
    TableManager_->Initialize();
    TabletManager_->Initialize();
    ChaosManager_->Initialize();
    MulticellManager_->Initialize();
    SchedulerPoolManager_->Initialize();

    // NB: Keep Config Manager initialization last and prevent
    // new automaton parts registration after its initialization.
    // Cf. TConfigManager::Initialize.
    ConfigManager_->Initialize();

    CellDirectorySynchronizer_ = New<NHiveServer::TCellDirectorySynchronizer>(
        Config_->CellDirectorySynchronizer,
        CellDirectory_,
        TamedCellManager_,
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::CellDirectorySynchronizer));
    CellDirectorySynchronizer_->Start();

    DiscoveryQueue_ = New<TActionQueue>("Discovery");
    auto discoveryServerConfig = New<TDiscoveryServerConfig>();
    discoveryServerConfig->ServerAddresses = std::move(addresses);
    DiscoveryServer_ = CreateDiscoveryServer(
        RpcServer_,
        localAddress,
        discoveryServerConfig,
        ChannelFactory_,
        DiscoveryQueue_->GetInvoker(),
        DiscoveryQueue_->GetInvoker());

    if (TimestampManager_) {
        RpcServer_->RegisterService(TimestampManager_->GetRpcService()); // null realm
    }
    RpcServer_->RegisterService(HiveManager_->GetRpcService()); // cell realm
    for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
        RpcServer_->RegisterService(service); // cell realm
    }
    RpcServer_->RegisterService(CreateLocalSnapshotService(CellId_, fileSnapshotStore)); // cell realm
    RpcServer_->RegisterService(CreateNodeTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateDataNodeTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateExecNodeTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateCellarNodeTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateTabletNodeTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateObjectService(Config_->ObjectService, this)); // master hydra service
    RpcServer_->RegisterService(CreateJobTrackerService(this)); // master hydra service
    RpcServer_->RegisterService(CreateChunkService(this)); // master hydra service
    RpcServer_->RegisterService(CreateAdminService(GetControlInvoker(), CoreDumper_));
    RpcServer_->RegisterService(CreateTransactionService(this)); // master hydra service
    RpcServer_->RegisterService(CreateCypressService(this)); // master hydra service

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
    CypressManager_->RegisterHandler(CreateChunkMapTypeHandler(this, EObjectType::OldestPartMissingChunkMap));
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
    CypressManager_->RegisterHandler(CreateMasterTableSchemaMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTableTypeHandler(this));
    CypressManager_->RegisterHandler(CreateReplicatedTableTypeHandler(this));
    CypressManager_->RegisterHandler(CreateJournalTypeHandler(this));
    CypressManager_->RegisterHandler(CreateAccountMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateAccountResourceUsageLeaseMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateUserMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateGroupMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateNetworkProjectMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateProxyRoleMapTypeHandler(this, EObjectType::HttpProxyRoleMap));
    CypressManager_->RegisterHandler(CreateProxyRoleMapTypeHandler(this, EObjectType::RpcProxyRoleMap));
    CypressManager_->RegisterHandler(CreatePoolTreeMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateCellNodeTypeHandler(this));
    CypressManager_->RegisterHandler(CreateCellBundleMapTypeHandler(this, ECellarType::Chaos, EObjectType::ChaosCellBundleMap));
    CypressManager_->RegisterHandler(CreateCellMapTypeHandler(this, ECellarType::Chaos, EObjectType::ChaosCellMap));
    CypressManager_->RegisterHandler(CreateCellBundleMapTypeHandler(this, ECellarType::Tablet, EObjectType::TabletCellBundleMap));
    CypressManager_->RegisterHandler(CreateCellMapTypeHandler(this, ECellarType::Tablet, EObjectType::TabletCellMap));
    CypressManager_->RegisterHandler(CreateTabletMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateTabletActionMapTypeHandler(this));
    CypressManager_->RegisterHandler(CreateAreaMapTypeHandler(this));

    CypressManager_->RegisterHandler(CreateEstimatedCreationTimeMapTypeHandler(this));

    RpcServer_->Configure(Config_->RpcServer);

    DiskSpaceProfiler_ = New<TDiskSpaceProfiler>(Config_);
    TProfiler{""}.AddProducer("", DiskSpaceProfiler_);
}

void TBootstrap::InitializeTimestampProvider()
{
    if (!Config_->EnableNetworking) {
        TimestampProvider_ = CreateNoopTimestampProvider();
        return;
    }

    auto timestampProviderChannel = CreateTimestampProviderChannel(Config_->TimestampProvider, ChannelFactory_);
    if (MulticellManager_->IsPrimaryMaster() && !Config_->EnableTimestampManager) {
        TimestampProvider_ = CreateBatchingRemoteTimestampProvider(
            Config_->TimestampProvider,
            std::move(timestampProviderChannel));
        RpcServer_->RegisterService(CreateTimestampProxyService(TimestampProvider_));
    } else {
        TimestampProvider_ = CreateRemoteTimestampProvider(
            Config_->TimestampProvider,
            std::move(timestampProviderChannel));
    }
}

void TBootstrap::DoRun()
{
    ClusterConnection_ = CreateClusterConnection();

    // Initialize periodic update of latest timestamp.
    TimestampProvider_->GetLatestTimestamp();

    DiscoveryServer_->Initialize();

    HydraFacade_->Initialize();

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);

    MonitoringManager_->Register(
        "/hydra",
        HydraFacade_->GetHydraManager()->GetMonitoringProducer());
    MonitoringManager_->Register(
        "/election",
        HydraFacade_->GetElectionManager()->GetMonitoringProducer());

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConvertTo<INodePtr>(Config_));
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
    SetNodeByYPath(
        orchidRoot,
        "/reign",
        ConvertTo<INodePtr>(GetCurrentReign()));
    SetBuildAttributes(
        orchidRoot,
        "master");

    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->RegisterService(CreateOrchidService(orchidRoot, GetControlInvoker()));
    RpcServer_->Start();
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

void TBootstrap::OnDynamicConfigChanged(const TDynamicClusterConfigPtr& /*oldConfig*/)
{
    const auto& config = ConfigManager_->GetConfig();
    ReconfigureSingletons(Config_, config->CellMaster);

    HydraFacade_->Reconfigure(config->CellMaster);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
