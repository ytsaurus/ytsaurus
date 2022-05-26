#pragma once

#include "public.h"

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/hive/public.h>

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

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/hydra2/public.h>

#include <yt/yt/server/lib/timestamp_server/public.h>

#include <yt/yt/server/lib/discovery_server/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/monitoring/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/profiling/profiler.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap() = default;

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
    const NRpc::IServerPtr& GetRpcServer() const;
    const NRpc::IChannelPtr& GetLocalRpcChannel() const;
    const NApi::NNative::IConnectionPtr& GetClusterConnection() const;
    const NElection::TCellManagerPtr& GetCellManager() const;
    const NHydra::IChangelogStoreFactoryPtr& GetChangelogStoreFactory() const;
    const NHydra::ISnapshotStorePtr& GetSnapshotStore() const;
    const NNodeTrackerServer::INodeTrackerPtr& GetNodeTracker() const;
    const NChunkServer::IDataNodeTrackerPtr& GetDataNodeTracker() const;
    const NNodeTrackerServer::IExecNodeTrackerPtr& GetExecNodeTracker() const;
    const NCellServer::ICellarNodeTrackerPtr& GetCellarNodeTracker() const;
    const NTabletServer::ITabletNodeTrackerPtr& GetTabletNodeTracker() const;
    const NTransactionServer::TTransactionManagerPtr& GetTransactionManager() const;
    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const;
    const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() const;
    const NCypressServer::TCypressManagerPtr& GetCypressManager() const;
    const NCypressServer::TPortalManagerPtr& GetPortalManager() const;
    const IHydraFacadePtr& GetHydraFacade() const;
    const IEpochHistoryManagerPtr& GetEpochHistoryManager() const;
    const IWorldInitializerPtr& GetWorldInitializer() const;
    const NObjectServer::IObjectManagerPtr& GetObjectManager() const;
    const NObjectServer::IYsonInternRegistryPtr& GetYsonInternRegistry() const;
    const NObjectServer::IRequestProfilingManagerPtr& GetRequestProfilingManager() const;
    const NChunkServer::TChunkManagerPtr& GetChunkManager() const;
    const NJournalServer::IJournalManagerPtr& GetJournalManager() const;
    const NSecurityServer::ISecurityManagerPtr& GetSecurityManager() const;
    const NSchedulerPoolServer::ISchedulerPoolManagerPtr& GetSchedulerPoolManager() const;
    const NCellServer::ITamedCellManagerPtr& GetTamedCellManager() const;
    const NTableServer::ITableManagerPtr& GetTableManager() const;
    const NTabletServer::TTabletManagerPtr& GetTabletManager() const;
    const NTabletServer::IBackupManagerPtr& GetBackupManager() const;
    const NChaosServer::IChaosManagerPtr& GetChaosManager() const;
    const NSequoiaServer::ISequoiaManagerPtr& GetSequoiaManager() const; 
    const NHiveServer::THiveManagerPtr& GetHiveManager() const;
    const NHiveClient::ICellDirectoryPtr& GetCellDirectory() const;
    const IInvokerPtr& GetControlInvoker() const;
    const NNodeTrackerClient::INodeChannelFactoryPtr& GetNodeChannelFactory() const;

    NDistributedThrottler::IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
        NDistributedThrottler::TDistributedThrottlerConfigPtr config,
        IInvokerPtr invoker,
        const TString& groupIdPrefix,
        NLogging::TLogger logger) const;

    void Initialize();
    void Run();
    void TryLoadSnapshot(
        const TString& fileName,
        bool dump,
        bool EnableTotalWriteCountReport,
        const TString& dumpConfigString);

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
    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NApi::NNative::IConnectionPtr ClusterConnection_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    NNodeTrackerServer::INodeTrackerPtr NodeTracker_;
    NChunkServer::IDataNodeTrackerPtr DataNodeTracker_;
    NNodeTrackerServer::IExecNodeTrackerPtr ExecNodeTracker_;
    NCellServer::ICellarNodeTrackerPtr CellarNodeTracker_;
    NTabletServer::ITabletNodeTrackerPtr TabletNodeTracker_;
    NTransactionServer::TTransactionManagerPtr TransactionManager_;
    NTimestampServer::TTimestampManagerPtr TimestampManager_;
    NTransactionClient::ITimestampProviderPtr TimestampProvider_;
    NHiveServer::ITransactionSupervisorPtr TransactionSupervisor_;
    NCypressServer::TCypressManagerPtr CypressManager_;
    NCypressServer::TPortalManagerPtr PortalManager_;
    IHydraFacadePtr HydraFacade_;
    IEpochHistoryManagerPtr EpochHistoryManager_;
    IWorldInitializerPtr WorldInitializer_;
    NObjectServer::IObjectManagerPtr ObjectManager_;
    NObjectServer::IObjectServicePtr ObjectService_;
    NObjectServer::IYsonInternRegistryPtr YsonInternRegistry_;
    NObjectServer::IRequestProfilingManagerPtr RequestProfilingManager_;
    NChunkServer::TChunkManagerPtr ChunkManager_;
    NJournalServer::IJournalManagerPtr JournalManager_;
    NSecurityServer::ISecurityManagerPtr SecurityManager_;
    NCellServer::ITamedCellManagerPtr TamedCellManager_;
    NCellServer::ICellHydraJanitorPtr CellHydraJanitor_;
    NTableServer::ITableManagerPtr TableManager_;
    NTabletServer::TTabletManagerPtr TabletManager_;
    NTabletServer::IBackupManagerPtr BackupManager_;
    NSchedulerPoolServer::ISchedulerPoolManagerPtr SchedulerPoolManager_;
    NTabletServer::TReplicatedTableTrackerPtr ReplicatedTableTracker_;
    NChaosServer::IChaosManagerPtr ChaosManager_;
    NSequoiaServer::ISequoiaManagerPtr SequoiaManager_;
    NHiveServer::THiveManagerPtr HiveManager_;
    NHiveClient::ICellDirectoryPtr CellDirectory_;
    NHiveServer::ICellDirectorySynchronizerPtr CellDirectorySynchronizer_;
    NConcurrency::TActionQueuePtr ControlQueue_;
    ICoreDumperPtr CoreDumper_;
    NConcurrency::TActionQueuePtr DiscoveryQueue_;
    NDiscoveryServer::IDiscoveryServerPtr DiscoveryServer_;
    NRpc::IChannelFactoryPtr ChannelFactory_;
    TDiskSpaceProfilerPtr DiskSpaceProfiler_;

    NNodeTrackerClient::INodeChannelFactoryPtr NodeChannelFactory_;

    static NElection::TPeerId ComputePeerId(
        NElection::TCellConfigPtr config,
        const TString& localAddress);

    NObjectClient::TCellTagList GetKnownParticipantCellTags() const;
    NApi::NNative::IConnectionPtr CreateClusterConnection() const;

    void DoInitialize();
    void InitializeTimestampProvider();
    void DoRun();
    void DoLoadSnapshot(
        const TString& fileName,
        bool dump,
        bool enableTotalWriteCountReport,
        const TSerializationDumperConfigPtr& dumpConfig);

    void ValidateLoadSnapshotParameters(
        bool dump,
        bool enableTotalWriteCountReport,
        const TString& dumpConfigString,
        TSerializationDumperConfigPtr* dumpConfig);

    void OnDynamicConfigChanged(const TDynamicClusterConfigPtr& oldConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
