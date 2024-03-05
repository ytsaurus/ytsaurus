#include "bootstrap.h"

#include "alert_manager.h"
#include "config.h"
#include "config_manager.h"
#include "epoch_history_manager.h"
#include "hydra_facade.h"
#include "master_hydra_service.h"
#include "multicell_manager.h"
#include "response_keeper_manager.h"
#include "private.h"
#include "world_initializer.h"

#include <yt/yt/server/master/chaos_server/chaos_manager.h>
#include <yt/yt/server/master/chaos_server/chaos_service.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/chunk_service.h>
#include <yt/yt/server/master/chunk_server/data_node_tracker.h>
#include <yt/yt/server/master/chunk_server/data_node_tracker_service.h>
#include <yt/yt/server/master/chunk_server/job_tracker_service.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/grafting_manager.h>
#include <yt/yt/server/master/cypress_server/portal_manager.h>
#include <yt/yt/server/master/cypress_server/sequoia_actions_executor.h>

#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>
#include <yt/yt/server/master/incumbent_server/incumbent_service.h>

#include <yt/yt/server/master/maintenance_tracker_server/maintenance_tracker.h>

#include <yt/yt/server/master/zookeeper_server/bootstrap_proxy.h>
#include <yt/yt/server/master/zookeeper_server/zookeeper_manager.h>

#include <yt/yt/server/lib/hive/avenue_directory.h>
#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_manager.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>
#include <yt/yt/server/lib/transaction_supervisor/transaction_participant_provider.h>

#include <yt/yt/server/master/hive/cell_directory_synchronizer.h>

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/file_changelog_dispatcher.h>
#include <yt/yt/server/lib/hydra/file_changelog.h>
#include <yt/yt/server/lib/hydra/local_changelog_store.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>
#include <yt/yt/server/lib/hydra/snapshot.h>
#include <yt/yt/server/lib/hydra/dry_run/dry_run_hydra_manager.h>
#include <yt/yt/server/lib/hydra/dry_run/public.h>

#include <yt/yt/server/lib/discovery_server/config.h>
#include <yt/yt/server/lib/discovery_server/discovery_server.h>

#include <yt/yt/server/lib/zookeeper_master/bootstrap.h>
#include <yt/yt/server/lib/zookeeper_master/bootstrap_proxy.h>

#include <yt/yt/server/master/journal_server/journal_manager.h>
#include <yt/yt/server/master/journal_server/journal_node.h>

#include <yt/yt/server/master/cell_server/cell_tracker_service.h>
#include <yt/yt/server/master/cell_server/cell_hydra_janitor.h>
#include <yt/yt/server/master/cell_server/cell_hydra_janitor.h>
#include <yt/yt/server/master/cell_server/cellar_node_tracker.h>
#include <yt/yt/server/master/cell_server/cellar_node_tracker_service.h>
#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/node_tracker_server/exec_node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/exec_node_tracker_service.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker_service.h>

#include <yt/yt/server/master/object_server/object_manager.h>
#include <yt/yt/server/master/object_server/object_service.h>
#include <yt/yt/server/master/object_server/request_profiling_manager.h>
#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool.h>
#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool_manager.h>

#include <yt/yt/server/master/security_server/config.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/sequoia_server/sequoia_manager.h>
#include <yt/yt/server/master/sequoia_server/sequoia_transaction_service.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/tablet_server/backup_manager.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>
#include <yt/yt/server/master/tablet_server/replicated_table_tracker.h>
#include <yt/yt/server/master/tablet_server/tablet_hydra_service.h>
#include <yt/yt/server/master/tablet_server/tablet_node_tracker.h>
#include <yt/yt/server/master/tablet_server/tablet_node_tracker_service.h>
#include <yt/yt/server/master/tablet_server/replicated_table_tracker_service.h>

#include <yt/yt/server/master/transaction_server/cypress_transaction_service.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>
#include <yt/yt/server/master/transaction_server/transaction_service.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/lease_server/lease_manager.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/server/lib/timestamp_server/timestamp_manager.h>

#include <yt/yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/auth/native_authenticating_channel.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/election/config.h>
#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/discovery_client/config.h>

#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/ytlib/sequoia_client/lazy_client.h>

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

#include <yt/yt/library/coredumper/coredumper.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

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
using namespace NCellServer;
using namespace NChaosServer;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NDiscoveryServer;
using namespace NDistributedThrottler;
using namespace NElection;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NIncumbentServer;
using namespace NJournalServer;
using namespace NLeaseServer;
using namespace NMaintenanceTrackerServer;
using namespace NMonitoring;
using namespace NNet;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NSchedulerPoolServer;
using namespace NSecurityServer;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NTableServer;
using namespace NTabletServer;
using namespace NTimestampServer;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYTree;
using namespace NZookeeperMaster;
using namespace NZookeeperServer;

using NTransactionServer::ITransactionManagerPtr;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("Bootstrap");
static inline const NLogging::TLogger DryRunLogger("DryRun");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap() = default;

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

void TBootstrap::VerifyPersistentStateRead() const
{
    HydraFacade_->VerifyPersistentStateRead();
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

const IConfigManagerPtr& TBootstrap::GetConfigManager() const
{
    return ConfigManager_;
}

const IMulticellManagerPtr& TBootstrap::GetMulticellManager() const
{
    return MulticellManager_;
}

const IIncumbentManagerPtr& TBootstrap::GetIncumbentManager() const
{
    return IncumbentManager_;
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

const NNative::IClientPtr& TBootstrap::GetRootClient() const
{
    return RootClient_;
}

ISequoiaClientPtr TBootstrap::GetSequoiaClient() const
{
    return SequoiaClient_;
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

const IMaintenanceTrackerPtr& TBootstrap::GetMaintenanceTracker() const
{
    return MaintenanceTracker_;
}

const INodeTrackerPtr& TBootstrap::GetNodeTracker() const
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

const ITransactionManagerPtr& TBootstrap::GetTransactionManager() const
{
    return TransactionManager_;
}

const ITransactionSupervisorPtr& TBootstrap::GetTransactionSupervisor() const
{
    return TransactionSupervisor_;
}

const ILeaseManagerPtr& TBootstrap::GetLeaseManager() const
{
    return LeaseManager_;
}

const ITimestampProviderPtr& TBootstrap::GetTimestampProvider() const
{
    return TimestampProvider_;
}

const ICypressManagerPtr& TBootstrap::GetCypressManager() const
{
    return CypressManager_;
}

const IPortalManagerPtr& TBootstrap::GetPortalManager() const
{
    return PortalManager_;
}

const IGraftingManagerPtr& TBootstrap::GetGraftingManager() const
{
    return GraftingManager_;
}

const IHydraFacadePtr& TBootstrap::GetHydraFacade() const
{
    return HydraFacade_;
}

const IEpochHistoryManagerPtr& TBootstrap::GetEpochHistoryManager() const
{
    return EpochHistoryManager_;
}

const IWorldInitializerPtr& TBootstrap::GetWorldInitializer() const
{
    return WorldInitializer_;
}

const IObjectManagerPtr& TBootstrap::GetObjectManager() const
{
    return ObjectManager_;
}

const IYsonInternRegistryPtr& TBootstrap::GetYsonInternRegistry() const
{
    return YsonInternRegistry_;
}

const IRequestProfilingManagerPtr& TBootstrap::GetRequestProfilingManager() const
{
    return RequestProfilingManager_;
}

const IChunkManagerPtr& TBootstrap::GetChunkManager() const
{
    return ChunkManager_;
}

const IJournalManagerPtr& TBootstrap::GetJournalManager() const
{
    return JournalManager_;
}

const ISecurityManagerPtr& TBootstrap::GetSecurityManager() const
{
    return SecurityManager_;
}

const ISchedulerPoolManagerPtr& TBootstrap::GetSchedulerPoolManager() const
{
    return SchedulerPoolManager_;
}

const ITableManagerPtr& TBootstrap::GetTableManager() const
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

const IBackupManagerPtr& TBootstrap::GetBackupManager() const
{
    return BackupManager_;
}

const IChaosManagerPtr& TBootstrap::GetChaosManager() const
{
    return ChaosManager_;
}

const ISequoiaManagerPtr& TBootstrap::GetSequoiaManager() const
{
    return SequoiaManager_;
}

const IHiveManagerPtr& TBootstrap::GetHiveManager() const
{
    return HiveManager_;
}

const ICellDirectoryPtr& TBootstrap::GetCellDirectory() const
{
    return CellDirectory_;
}

const TSimpleAvenueDirectoryPtr& TBootstrap::GetAvenueDirectory() const
{
    return AvenueDirectory_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetSnapshotIOInvoker() const
{
    return SnapshotIOQueue_->GetInvoker();
}

const INodeChannelFactoryPtr& TBootstrap::GetNodeChannelFactory() const
{
    return NodeChannelFactory_;
}

const IReplicatedTableTrackerStateProviderPtr& TBootstrap::GetReplicatedTableTrackerStateProvider() const
{
    return ReplicatedTableTrackerStateProvider_;
}

const NRpc::IAuthenticatorPtr& TBootstrap::GetNativeAuthenticator() const
{
    return NativeAuthenticator_;
}

const NZookeeperServer::IZookeeperManagerPtr& TBootstrap::GetZookeeperManager() const
{
    return ZookeeperManager_;
}

NZookeeperMaster::IBootstrap* TBootstrap::GetZookeeperBootstrap() const
{
    return ZookeeperBootstrap_.get();
}

NDistributedThrottler::IDistributedThrottlerFactoryPtr TBootstrap::CreateDistributedThrottlerFactory(
    TDistributedThrottlerConfigPtr config,
    IInvokerPtr invoker,
    const TString& groupIdPrefix,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler) const
{
    return NDistributedThrottler::CreateDistributedThrottlerFactory(
        std::move(config),
        ChannelFactory_,
        ClusterConnection_,
        std::move(invoker),
        Format("%v/%v", groupIdPrefix, CellTag_),
        ToString(GetCellManager()->GetSelfPeerId()),
        RpcServer_,
        BuildServiceAddress(GetLocalHostName(), Config_->RpcPort),
        std::move(logger),
        NativeAuthenticator_,
        profiler);
}

void TBootstrap::Initialize()
{
    ControlQueue_ = New<TActionQueue>("Control");
    SnapshotIOQueue_ = New<TActionQueue>("SnapshotIO");

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

void TBootstrap::LoadSnapshotOrThrow(
    const TString& fileName,
    bool dump,
    bool enableTotalWriteCountReport,
    const TString& dumpConfigString)
{
    TSerializationDumperConfigPtr dumpConfig;
    ValidateLoadSnapshotParameters(dump, enableTotalWriteCountReport, dumpConfigString, &dumpConfig);

    BIND(&TBootstrap::DoLoadSnapshot, this, fileName, dump, enableTotalWriteCountReport, dumpConfig)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrap::ReplayChangelogsOrThrow(std::vector<TString> changelogFileNames)
{
    BIND(&TBootstrap::DoReplayChangelogs, this, Passed(std::move(changelogFileNames)))
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrap::BuildSnapshotOrThrow()
{
    BIND(&TBootstrap::DoBuildSnapshot, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrap::FinishDryRunOrThrow()
{
    BIND(&TBootstrap::DoFinishDryRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TCellTagList TBootstrap::GetKnownParticipantCellTags() const
{
    TCellTagList participantCellTags;
    participantCellTags.push_back(PrimaryCellTag_);
    participantCellTags.insert(participantCellTags.end(), SecondaryCellTags_.begin(), SecondaryCellTags_.end());
    return participantCellTags;
}

class TDiskSpaceProfiler
    : public NProfiling::ISensorProducer
{
public:
    explicit TDiskSpaceProfiler(TCellMasterConfigPtr config)
        : Config_(std::move(config))
    { }

    void CollectSensors(ISensorWriter* writer) override
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

    auto actualLocalHostName = GetLocalHostName();

    // Override value always takes priority.
    // If expected local host name is not set we just skip host name validation.
    auto expectedLocalHostName = Config_->AddressResolver->LocalHostNameOverride.value_or(
        Config_->AddressResolver->ExpectedLocalHostName.value_or(actualLocalHostName));

    if (Config_->DryRun->EnableHostNameValidation &&
        expectedLocalHostName != actualLocalHostName)
    {
        THROW_ERROR_EXCEPTION("Local address differs from expected address specified in config")
            << TErrorAttribute("local_address", actualLocalHostName)
            << TErrorAttribute("localhost_name", Config_->AddressResolver->ExpectedLocalHostName)
            << TErrorAttribute("localhost_name_override", Config_->AddressResolver->LocalHostNameOverride);
    }

    auto localAddress = BuildServiceAddress(expectedLocalHostName, Config_->RpcPort);

    TCellConfigPtr localCellConfig;
    int localPeerId;

    auto primaryId = Config_->PrimaryMaster->FindPeerId(localAddress);
    if (primaryId == InvalidPeerId) {
        for (auto cellConfig : Config_->SecondaryMasters) {
            auto secondaryId = cellConfig->FindPeerId(localAddress);
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

    ClusterConnection_ = NNative::CreateConnection(Config_->ClusterConnection);

    RootClient_ = ClusterConnection_->CreateNativeClient(NApi::TClientOptions::FromUser(NSecurityClient::RootUserName));

    SequoiaClient_ = CreateLazySequoiaClient(RootClient_, Logger);

    // If Sequoia is local it's safe to create the client right now.
    const auto& groundClusterName = Config_->ClusterConnection->Dynamic->SequoiaConnection->GroundClusterName;
    if (!groundClusterName) {
        SequoiaClient_->SetGroundClient(RootClient_);
    }

    NativeAuthenticator_ = NNative::CreateNativeAuthenticator(ClusterConnection_);

    ChannelFactory_ = NAuth::CreateNativeAuthenticationInjectingChannelFactory(
        CreateCachingChannelFactory(
            NRpc::NBus::CreateTcpBusChannelFactory(Config_->BusClient)),
        Config_->ClusterConnection->Dynamic->TvmId);

    const auto& networks = Config_->Networks;

    NodeChannelFactory_ = CreateNodeChannelFactory(ChannelFactory_, networks);

    CellDirectory_ = CreateCellDirectory(
        Config_->CellDirectory,
        ChannelFactory_,
        ClusterConnection_->GetClusterDirectory(),
        networks,
        Logger);

    YT_VERIFY(CellDirectory_->ReconfigureCell(Config_->PrimaryMaster));
    for (const auto& cellConfig : Config_->SecondaryMasters) {
        YT_VERIFY(CellDirectory_->ReconfigureCell(cellConfig));
    }

    AvenueDirectory_ = New<TSimpleAvenueDirectory>();

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    auto busServer = CreateBusServer(Config_->BusServer);

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

    auto snapshotStoreFuture = CreateLocalSnapshotStore(
        Config_->Snapshots,
        GetSnapshotIOInvoker());
    auto snapshotStore = WaitFor(snapshotStoreFuture)
        .ValueOrThrow();
    SnapshotStore_ = snapshotStore;

    HydraFacade_ = CreateHydraFacade(this);

    AlertManager_ = CreateAlertManager(this);

    ConfigManager_ = CreateConfigManager(this);
    ConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

    EpochHistoryManager_ = CreateEpochHistoryManager(this);

    MulticellManager_ = CreateMulticellManager(this);

    WorldInitializer_ = CreateWorldInitializer(this);

    IncumbentManager_ = CreateIncumbentManager(this);

    HiveManager_ = CreateHiveManager(
        Config_->HiveManager,
        CellDirectory_,
        AvenueDirectory_,
        CellId_,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::HiveManager),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton(),
        CreateMulticellUpstreamSynchronizer(this),
        NativeAuthenticator_);

    std::vector<TString> addresses;
    addresses.reserve(localCellConfig->Peers.size());
    for (const auto& peer : localCellConfig->Peers) {
        if (peer->Address) {
            addresses.push_back(*peer->Address);
        }
    }

    // NB: This is exactly the order in which parts get registered and there are some
    // dependencies in Clear methods.
    ResponseKeeperManager_ = CreateResponseKeeperManager(this);

    ObjectManager_ = CreateObjectManager(this);

    YsonInternRegistry_ = CreateYsonInternRegistry(this);

    RequestProfilingManager_ = CreateRequestProfilingManager();

    SecurityManager_ = CreateSecurityManager(this);

    TransactionManager_ = CreateTransactionManager(this);

    MaintenanceTracker_ = CreateMaintenanceTracker(this);

    NodeTracker_ = CreateNodeTracker(this);

    DataNodeTracker_ = CreateDataNodeTracker(this);

    ExecNodeTracker_ = CreateExecNodeTracker(this);

    CellarNodeTracker_ = CreateCellarNodeTracker(this);

    TabletNodeTracker_ = CreateTabletNodeTracker(this);

    CypressManager_ = CreateCypressManager(this);

    PortalManager_ =  CreatePortalManager(this);

    GraftingManager_ = CreateGraftingManager(this);

    SequoiaActionsExecutor_ = CreateSequoiaActionsExecutor(this);

    ChunkManager_ = CreateChunkManager(this);

    JournalManager_ = CreateJournalManager(this);

    TamedCellManager_ = CreateTamedCellManager(this);

    CellHydraJanitor_ = CreateCellHydraJanitor(this);

    TableManager_ = CreateTableManager(this);

    TabletManager_ = New<TTabletManager>(this);

    BackupManager_ = CreateBackupManager(this);

    ChaosManager_ = CreateChaosManager(this);

    SequoiaManager_ = CreateSequoiaManager(this);

    ReplicatedTableTracker_ = New<TReplicatedTableTracker>(Config_->ReplicatedTableTracker, this);

    SchedulerPoolManager_ = CreateSchedulerPoolManager(this);

    ObjectService_ = CreateObjectService(Config_->ObjectService, this);

    ZookeeperBootstrapProxy_ = CreateZookeeperBootstrapProxy(this);
    ZookeeperBootstrap_ = CreateBootstrap(ZookeeperBootstrapProxy_.get());

    ZookeeperManager_ = CreateZookeeperManager(this);

    InitializeTimestampProvider();

    if (MulticellManager_->IsPrimaryMaster() && Config_->EnableTimestampManager) {
        TimestampManager_ = New<TTimestampManager>(
            Config_->TimestampManager,
            HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TimestampManager),
            HydraFacade_->GetHydraManager(),
            HydraFacade_->GetAutomaton(),
            GetCellTag(),
            /*authenticator*/ nullptr);
    }

    LeaseManager_ = CreateLeaseManager(
        Config_->LeaseManager,
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton(),
        HiveManager_,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::LeaseManager),
        CellId_,
        // NB: There is no need for multicell syncs in lease manager.
        CreateHydraManagerUpstreamSynchronizer(HydraFacade_->GetHydraManager()),
        NativeAuthenticator_);

    AlertManager_->Initialize();
    ObjectManager_->Initialize();
    // Recalculates roles for master cells.
    // If you need to know cell roles, initialize it below MulticellManager_.
    MulticellManager_->Initialize();
    IncumbentManager_->Initialize();
    SecurityManager_->Initialize();
    TransactionManager_->Initialize();
    NodeTracker_->Initialize();
    DataNodeTracker_->Initialize();
    ExecNodeTracker_->Initialize();
    CellarNodeTracker_->Initialize();
    TabletNodeTracker_->Initialize();
    CypressManager_->Initialize();
    PortalManager_->Initialize();
    ChunkManager_->Initialize();
    TamedCellManager_->Initialize();
    CellHydraJanitor_->Initialize();
    TableManager_->Initialize();
    TabletManager_->Initialize();
    BackupManager_->Initialize();
    ChaosManager_->Initialize();
    SchedulerPoolManager_->Initialize();
    ZookeeperBootstrap_->Initialize();
    ZookeeperManager_->Initialize();
    GraftingManager_->Initialize();
    SequoiaActionsExecutor_->Initialize();

    // NB: Keep Config Manager initialization last and prevent
    // new automaton parts registration after its initialization.
    // Cf. TConfigManager::Initialize.
    ConfigManager_->Initialize();

    // NB: We rely on the config manager signal being called after RTT initialization so actual config will be applied.
    ReplicatedTableTrackerActionQueue_ = New<TActionQueue>("RttQueue");
    auto rttInvoker = ReplicatedTableTrackerActionQueue_->GetInvoker();
    ReplicatedTableTrackerStateProvider_ = CreateReplicatedTableTrackerStateProvider(this);

    CellDirectorySynchronizer_ = CreateCellDirectorySynchronizer(
        Config_->CellDirectorySynchronizer,
        CellDirectory_,
        TamedCellManager_,
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::CellDirectorySynchronizer));
    CellDirectorySynchronizer_->Start();

    auto localTransactionParticipantProvider = CreateTransactionParticipantProvider(
        CellDirectory_,
        CellDirectorySynchronizer_,
        TimestampProvider_,
        GetKnownParticipantCellTags());

    auto transactionParticipantProviders = std::vector{std::move(localTransactionParticipantProvider)};

    if (groundClusterName) {
        auto remoteTransactionParticipantProvider = CreateTransactionParticipantProvider(ClusterConnection_->GetClusterDirectory());
        transactionParticipantProviders.push_back(std::move(remoteTransactionParticipantProvider));
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
        PrimaryCellTag_,
        TimestampProvider_,
        std::move(transactionParticipantProviders),
        NativeAuthenticator_);

    DiscoveryQueue_ = New<TActionQueue>("Discovery");
    auto discoveryServerConfig = New<TDiscoveryServerConfig>();
    discoveryServerConfig->ServerAddresses = std::move(addresses);
    DiscoveryServer_ = CreateDiscoveryServer(
        RpcServer_,
        localAddress,
        discoveryServerConfig,
        ChannelFactory_,
        DiscoveryQueue_->GetInvoker(),
        DiscoveryQueue_->GetInvoker(),
        NativeAuthenticator_);

    if (TimestampManager_) {
        RpcServer_->RegisterService(TimestampManager_->GetRpcService()); // null realm
    }
    RpcServer_->RegisterService(HiveManager_->GetRpcService()); // cell realm
    for (const auto& service : TransactionSupervisor_->GetRpcServices()) {
        RpcServer_->RegisterService(service); // cell realm
    }

    RpcServer_->RegisterService(CreateNodeTrackerService(this));
    RpcServer_->RegisterService(CreateDataNodeTrackerService(this));
    RpcServer_->RegisterService(CreateExecNodeTrackerService(this));
    RpcServer_->RegisterService(CreateCellarNodeTrackerService(this));
    RpcServer_->RegisterService(CreateTabletNodeTrackerService(this));
    RpcServer_->RegisterService(ObjectService_);
    RpcServer_->RegisterService(CreateJobTrackerService(this));
    RpcServer_->RegisterService(CreateChunkService(this));
    RpcServer_->RegisterService(CreateAdminService(GetControlInvoker(), CoreDumper_, NativeAuthenticator_));
    RpcServer_->RegisterService(CreateTransactionService(this));
    RpcServer_->RegisterService(CreateCypressTransactionService(this));
    RpcServer_->RegisterService(CreateMasterChaosService(this));
    RpcServer_->RegisterService(CreateCellTrackerService(this));
    RpcServer_->RegisterService(CreateSequoiaTransactionService(this));
    RpcServer_->RegisterService(CreateIncumbentService(this));
    RpcServer_->RegisterService(CreateTabletHydraService(this));
    RpcServer_->RegisterService(CreateReplicatedTableTrackerService(this, rttInvoker));

    RpcServer_->Configure(Config_->RpcServer);

    DiskSpaceProfiler_ = New<TDiskSpaceProfiler>(Config_);
    TProfiler{""}.AddProducer("", DiskSpaceProfiler_);
}

void TBootstrap::InitializeTimestampProvider()
{
    if (MulticellManager_->IsPrimaryMaster() && !Config_->EnableTimestampManager) {
        TimestampProvider_ = CreateBatchingRemoteTimestampProvider(Config_->TimestampProvider, ChannelFactory_);

        RpcServer_->RegisterService(CreateTimestampProxyService(
            TimestampProvider_,
            /*alienProviders*/ {},
            /*authenticator*/ nullptr));
    } else {
        auto timestampProviderChannel = CreateTimestampProviderChannel(Config_->TimestampProvider, ChannelFactory_);
        TimestampProvider_ = CreateRemoteTimestampProvider(
            Config_->TimestampProvider,
            std::move(timestampProviderChannel));
    }
}

void TBootstrap::DoRun()
{
    if (const auto& groundClusterName = Config_->ClusterConnection->Dynamic->SequoiaConnection->GroundClusterName) {
        ClusterConnection_->GetClusterDirectory()->SubscribeOnClusterUpdated(
            BIND([=, this] (const TString& clusterName, const INodePtr& /*configNode*/) {
                if (clusterName == *groundClusterName) {
                    auto groundConnection = ClusterConnection_->GetClusterDirectory()->GetConnection(*groundClusterName);
                    auto groundClient = groundConnection->CreateNativeClient({.User = NSecurityClient::RootUserName});
                    SequoiaClient_->SetGroundClient(std::move(groundClient));
                }
            }));
    }
    ClusterConnection_->GetClusterDirectorySynchronizer()->Start();

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
        CreateVirtualNode(ConvertTo<INodePtr>(Config_)));
    SetNodeByYPath(
        orchidRoot,
        "/incumbent_manager",
        CreateVirtualNode(IncumbentManager_->GetOrchidService()));
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
        "/transaction_supervisor",
        CreateVirtualNode(TransactionSupervisor_->GetOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/discovery_server",
        CreateVirtualNode(DiscoveryServer_->GetYPathService()));
    SetNodeByYPath(
        orchidRoot,
        "/object_service_cache",
        CreateVirtualNode(ObjectService_->GetCache()->GetOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/tablet_manager",
        CreateVirtualNode(TabletManager_->GetOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/reign",
        ConvertTo<INodePtr>(GetCurrentReign()));
    SetBuildAttributes(
        orchidRoot,
        "master");

    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->RegisterService(CreateOrchidService(orchidRoot, GetControlInvoker(), NativeAuthenticator_));
    RpcServer_->Start();
}

void TBootstrap::DoLoadSnapshot(
    const TString& fileName,
    bool dump,
    bool enableTotalWriteCountReport,
    const TSerializationDumperConfigPtr& dumpConfig)
{
    auto snapshotId = TryFromString<int>(NFS::GetFileNameWithoutExtension(fileName));
    if (snapshotId.Empty()) {
        snapshotId = InvalidSegmentId;
        YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Can't parse snapshot name as id, using id %v as substitute",
            snapshotId);
    }
    auto snapshotReader = CreateLocalSnapshotReader(fileName, *snapshotId, GetSnapshotIOInvoker());

    const auto& hydraManager = HydraFacade_->GetHydraManager();
    auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);

    dryRunHydraManager->Initialize();

    const auto& automaton = HydraFacade_->GetAutomaton();
    automaton->SetSnapshotValidationOptions({dump, enableTotalWriteCountReport, dumpConfig});

    dryRunHydraManager->DryRunLoadSnapshot(std::move(snapshotReader), *snapshotId);
}

void TBootstrap::DoReplayChangelogs(const std::vector<TString>& changelogFileNames)
{
    const auto& hydraManager = HydraFacade_->GetHydraManager();
    auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);

    dryRunHydraManager->Initialize();

    auto changelogsConfig = Config_->Changelogs;

    changelogsConfig->Path = NFS::GetDirectoryName(changelogFileNames.front());
    auto ioEngine = CreateIOEngine(Config_->Changelogs->IOEngineType, Config_->Changelogs->IOConfig);

    auto dispatcher = CreateFileChangelogDispatcher(
        std::move(ioEngine),
        /*memoryUsageTracker*/ nullptr,
        changelogsConfig,
        "DryRunChangelogDispatcher",
        /*profiler*/ {});

    for (auto changelogFileName : changelogFileNames) {
        auto changelogId = TryFromString<int>(NFS::GetFileNameWithoutExtension(changelogFileName));
        if (changelogId.Empty()) {
            changelogId = InvalidSegmentId;
            YT_LOG_EVENT(DryRunLogger, NLogging::ELogLevel::Info, "Can't parse changelog name as id, using id %v as substitute",
                changelogId);
        }

        auto changelog = WaitFor(dispatcher->OpenChangelog(*changelogId, changelogFileName, changelogsConfig))
            .ValueOrThrow();
        dryRunHydraManager->DryRunReplayChangelog(changelog);
    }
}

void TBootstrap::DoBuildSnapshot()
{
    const auto& hydraManager = HydraFacade_->GetHydraManager();
    auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);
    dryRunHydraManager->Initialize();
    dryRunHydraManager->DryRunBuildSnapshot();
}

void TBootstrap::DoFinishDryRun()
{
    const auto& hydraManager = HydraFacade_->GetHydraManager();
    auto dryRunHydraManager = StaticPointerCast<IDryRunHydraManager>(hydraManager);
    dryRunHydraManager->DryRunShutdown();
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
    ReconfigureNativeSingletons(Config_, config->CellMaster);

    HydraFacade_->Reconfigure(config->CellMaster);

    const auto& testingConfig = config->MulticellManager->Testing;
    // TODO(cherepashka): temporary logic.
    if (testingConfig->MasterCellDirectoryOverride) {
        MulticellManager_->GetMasterCellConnectionConfigs()->SecondaryMasters = testingConfig->MasterCellDirectoryOverride->SecondaryMasters;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
