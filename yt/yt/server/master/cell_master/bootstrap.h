#pragma once

#include "public.h"

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/incumbent_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/hive/public.h>

#include <yt/yt/server/master/maintenance_tracker_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/scheduler_pool_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/sequoia_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/zookeeper_server/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/server/lib/tablet_server/public.h>

#include <yt/yt/server/lib/timestamp_server/public.h>

#include <yt/yt/server/lib/discovery_server/public.h>

#include <yt/yt/server/lib/zookeeper_master/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap();
    TBootstrap(TCellMasterConfigPtr config);

    ~TBootstrap();

    const TCellMasterConfigPtr& GetConfig() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;
    bool IsMulticell() const;

    void VerifyPersistentStateRead() const;

    NObjectClient::TCellId GetCellId() const;
    NObjectClient::TCellTag GetCellTag() const;

    NObjectClient::TCellId GetPrimaryCellId() const;
    NObjectClient::TCellTag GetPrimaryCellTag() const;

    const NObjectClient::TCellTagList& GetSecondaryCellTags() const;

    const IAlertManagerPtr& GetAlertManager() const;
    const IConfigManagerPtr& GetConfigManager() const;
    const IMulticellManagerPtr& GetMulticellManager() const;
    const NIncumbentServer::IIncumbentManagerPtr& GetIncumbentManager() const;
    const NRpc::IServerPtr& GetRpcServer() const;
    const NRpc::IChannelPtr& GetLocalRpcChannel() const;
    const NApi::NNative::IConnectionPtr& GetClusterConnection() const;
    const NApi::NNative::IClientPtr& GetRootClient() const;
    NSequoiaClient::ISequoiaClientPtr GetSequoiaClient() const;
    const NElection::TCellManagerPtr& GetCellManager() const;
    const NHydra::IChangelogStoreFactoryPtr& GetChangelogStoreFactory() const;
    const NHydra::ISnapshotStorePtr& GetSnapshotStore() const;
    const NMaintenanceTrackerServer::IMaintenanceTrackerPtr& GetMaintenanceTracker() const;
    const NNodeTrackerServer::INodeTrackerPtr& GetNodeTracker() const;
    const NChunkServer::IDataNodeTrackerPtr& GetDataNodeTracker() const;
    const NNodeTrackerServer::IExecNodeTrackerPtr& GetExecNodeTracker() const;
    const NCellServer::ICellarNodeTrackerPtr& GetCellarNodeTracker() const;
    const NTabletServer::ITabletNodeTrackerPtr& GetTabletNodeTracker() const;
    const NTransactionServer::ITransactionManagerPtr& GetTransactionManager() const;
    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const;
    const NTransactionSupervisor::ITransactionSupervisorPtr& GetTransactionSupervisor() const;
    const NLeaseServer::ILeaseManagerPtr& GetLeaseManager() const;
    const NCypressServer::ICypressManagerPtr& GetCypressManager() const;
    const NCypressServer::IPortalManagerPtr& GetPortalManager() const;
    const NCypressServer::IGraftingManagerPtr& GetGraftingManager() const;
    const IHydraFacadePtr& GetHydraFacade() const;
    const IEpochHistoryManagerPtr& GetEpochHistoryManager() const;
    const IWorldInitializerPtr& GetWorldInitializer() const;
    const NObjectServer::IObjectManagerPtr& GetObjectManager() const;
    const NObjectServer::IYsonInternRegistryPtr& GetYsonInternRegistry() const;
    const NObjectServer::IRequestProfilingManagerPtr& GetRequestProfilingManager() const;
    const NChunkServer::IChunkManagerPtr& GetChunkManager() const;
    const NJournalServer::IJournalManagerPtr& GetJournalManager() const;
    const NSecurityServer::ISecurityManagerPtr& GetSecurityManager() const;
    const NSchedulerPoolServer::ISchedulerPoolManagerPtr& GetSchedulerPoolManager() const;
    const NCellServer::ITamedCellManagerPtr& GetTamedCellManager() const;
    const NTableServer::ITableManagerPtr& GetTableManager() const;
    const NTabletServer::TTabletManagerPtr& GetTabletManager() const;
    const NTabletServer::IBackupManagerPtr& GetBackupManager() const;
    const NChaosServer::IChaosManagerPtr& GetChaosManager() const;
    const NSequoiaServer::ISequoiaManagerPtr& GetSequoiaManager() const;
    const NHiveServer::IHiveManagerPtr& GetHiveManager() const;
    const NHiveClient::ICellDirectoryPtr& GetCellDirectory() const;
    const NHiveServer::TSimpleAvenueDirectoryPtr& GetAvenueDirectory() const;
    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetSnapshotIOInvoker() const;
    const NNodeTrackerClient::INodeChannelFactoryPtr& GetNodeChannelFactory() const;
    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const;
    const NTabletServer::IReplicatedTableTrackerStateProviderPtr& GetReplicatedTableTrackerStateProvider() const;

    const NZookeeperServer::IZookeeperManagerPtr& GetZookeeperManager() const;
    NZookeeperMaster::IBootstrap* GetZookeeperBootstrap() const;

    NDistributedThrottler::IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
        NDistributedThrottler::TDistributedThrottlerConfigPtr config,
        IInvokerPtr invoker,
        const TString& groupIdPrefix,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler) const;

    void Initialize();
    void Run();
    void LoadSnapshotOrThrow(
        const TString& fileName,
        bool dump,
        bool EnableTotalWriteCountReport,
        const TString& dumpConfigString);

    void ReplayChangelogsOrThrow(std::vector<TString> changelogFileNames);

    void BuildSnapshotOrThrow();

    void FinishDryRunOrThrow();

protected:
    const TCellMasterConfigPtr Config_;

    bool PrimaryMaster_ = false;
    bool SecondaryMaster_ = false;
    bool Multicell_ = false;

    NObjectClient::TCellId CellId_;
    NObjectClient::TCellTag CellTag_;
    NObjectClient::TCellId PrimaryCellId_;
    NObjectClient::TCellTag PrimaryCellTag_;
    NObjectClient::TCellTagList SecondaryCellTags_;

    IAlertManagerPtr AlertManager_;
    IConfigManagerPtr ConfigManager_;
    IMulticellManagerPtr MulticellManager_;
    NIncumbentServer::IIncumbentManagerPtr IncumbentManager_;
    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NApi::NNative::IConnectionPtr ClusterConnection_;
    NApi::NNative::IClientPtr RootClient_;
    NSequoiaClient::ILazySequoiaClientPtr SequoiaClient_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    NMaintenanceTrackerServer::IMaintenanceTrackerPtr MaintenanceTracker_;
    NNodeTrackerServer::INodeTrackerPtr NodeTracker_;
    NChunkServer::IDataNodeTrackerPtr DataNodeTracker_;
    NNodeTrackerServer::IExecNodeTrackerPtr ExecNodeTracker_;
    NCellServer::ICellarNodeTrackerPtr CellarNodeTracker_;
    NTabletServer::ITabletNodeTrackerPtr TabletNodeTracker_;
    NTransactionServer::ITransactionManagerPtr TransactionManager_;
    NTimestampServer::TTimestampManagerPtr TimestampManager_;
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;
    NTransactionSupervisor::ITransactionSupervisorPtr TransactionSupervisor_;
    NLeaseServer::ILeaseManagerPtr LeaseManager_;
    NCypressServer::ICypressManagerPtr CypressManager_;
    NCypressServer::IPortalManagerPtr PortalManager_;
    NCypressServer::IGraftingManagerPtr GraftingManager_;
    NCypressServer::ISequoiaActionsExecutorPtr SequoiaActionsExecutor_;
    IHydraFacadePtr HydraFacade_;
    IEpochHistoryManagerPtr EpochHistoryManager_;
    IWorldInitializerPtr WorldInitializer_;
    IResponseKeeperManagerPtr ResponseKeeperManager_;
    NObjectServer::IObjectManagerPtr ObjectManager_;
    NObjectServer::IObjectServicePtr ObjectService_;
    NObjectServer::IYsonInternRegistryPtr YsonInternRegistry_;
    NObjectServer::IRequestProfilingManagerPtr RequestProfilingManager_;
    NChunkServer::IChunkManagerPtr ChunkManager_;
    NJournalServer::IJournalManagerPtr JournalManager_;
    NSecurityServer::ISecurityManagerPtr SecurityManager_;
    NCellServer::ITamedCellManagerPtr TamedCellManager_;
    NCellServer::ICellHydraJanitorPtr CellHydraJanitor_;
    NTableServer::ITableManagerPtr TableManager_;
    NTabletServer::TTabletManagerPtr TabletManager_;
    NTabletServer::IBackupManagerPtr BackupManager_;
    NSchedulerPoolServer::ISchedulerPoolManagerPtr SchedulerPoolManager_;
    NTabletServer::TReplicatedTableTrackerPtr ReplicatedTableTracker_;
    NConcurrency::TActionQueuePtr ReplicatedTableTrackerActionQueue_;
    NTabletServer::IReplicatedTableTrackerStateProviderPtr ReplicatedTableTrackerStateProvider_;
    NChaosServer::IChaosManagerPtr ChaosManager_;
    NSequoiaServer::ISequoiaManagerPtr SequoiaManager_;
    NHiveServer::IHiveManagerPtr HiveManager_;
    NHiveClient::ICellDirectoryPtr CellDirectory_;
    NHiveServer::TSimpleAvenueDirectoryPtr AvenueDirectory_;
    NHiveClient::ICellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    NConcurrency::TActionQueuePtr ControlQueue_;
    NConcurrency::TActionQueuePtr SnapshotIOQueue_;
    NCoreDump::ICoreDumperPtr CoreDumper_;
    NConcurrency::TActionQueuePtr DiscoveryQueue_;
    NDiscoveryServer::IDiscoveryServerPtr DiscoveryServer_;
    NRpc::IChannelFactoryPtr ChannelFactory_;
    TDiskSpaceProfilerPtr DiskSpaceProfiler_;

    std::unique_ptr<NZookeeperMaster::IBootstrapProxy> ZookeeperBootstrapProxy_;
    std::unique_ptr<NZookeeperMaster::IBootstrap> ZookeeperBootstrap_;

    NZookeeperServer::IZookeeperManagerPtr ZookeeperManager_;

    NNodeTrackerClient::INodeChannelFactoryPtr NodeChannelFactory_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TCallback<void(const TString &, NYTree::INodePtr)> GroundConnectionCallback_;

    NObjectClient::TCellTagList GetKnownParticipantCellTags() const;

    void DoInitialize();
    void InitializeTimestampProvider();
    void DoRun();
    void DoLoadSnapshot(
        const TString& fileName,
        bool dump,
        bool enableTotalWriteCountReport,
        const NHydra::TSerializationDumperConfigPtr& dumpConfig);

    void DoReplayChangelogs(const std::vector<TString>& changelogFileNames);

    void DoBuildSnapshot();

    void DoFinishDryRun();

    void ValidateLoadSnapshotParameters(
        bool dump,
        bool enableTotalWriteCountReport,
        const TString& dumpConfigString,
        NHydra::TSerializationDumperConfigPtr* dumpConfig);

    void OnDynamicConfigChanged(const TDynamicClusterConfigPtr& oldConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
