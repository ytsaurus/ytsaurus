#pragma once

#include "public.h"

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/scheduler_pool_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/public.h>

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
    TBootstrap(TCellMasterConfigPtr config);
    ~TBootstrap();

    const TCellMasterConfigPtr& GetConfig() const;

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;
    bool IsMulticell() const;

    NObjectClient::TCellId GetCellId() const;
    NObjectClient::TCellTag GetCellTag() const;

    NObjectClient::TCellId GetPrimaryCellId() const;
    NObjectClient::TCellTag GetPrimaryCellTag() const;

    const NObjectClient::TCellTagList& GetSecondaryCellTags() const;

    const IAlertManagerPtr& GetAlertManager() const;
    const TConfigManagerPtr& GetConfigManager() const;
    const TMulticellManagerPtr& GetMulticellManager() const;
    const NRpc::IServerPtr& GetRpcServer() const;
    const NRpc::IChannelPtr& GetLocalRpcChannel() const;
    const NApi::NNative::IConnectionPtr& GetClusterConnection() const;
    const NElection::TCellManagerPtr& GetCellManager() const;
    const NHydra::IChangelogStoreFactoryPtr& GetChangelogStoreFactory() const;
    const NHydra::ISnapshotStorePtr& GetSnapshotStore() const;
    const NNodeTrackerServer::TNodeTrackerPtr& GetNodeTracker() const;
    const NChunkServer::IDataNodeTrackerPtr& GetDataNodeTracker() const;
    const NNodeTrackerServer::IExecNodeTrackerPtr& GetExecNodeTracker() const;
    const NCellServer::ICellarNodeTrackerPtr& GetCellarNodeTracker() const;
    const NTabletServer::ITabletNodeTrackerPtr& GetTabletNodeTracker() const;
    const NTransactionServer::TTransactionManagerPtr& GetTransactionManager() const;
    const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() const;
    const NHiveServer::ITransactionSupervisorPtr& GetTransactionSupervisor() const;
    const NCypressServer::TCypressManagerPtr& GetCypressManager() const;
    const NCypressServer::TPortalManagerPtr& GetPortalManager() const;
    const THydraFacadePtr& GetHydraFacade() const;
    const TEpochHistoryManagerPtr& GetEpochHistoryManager() const;
    const TWorldInitializerPtr& GetWorldInitializer() const;
    const NObjectServer::TObjectManagerPtr& GetObjectManager() const;
    const NObjectServer::IYsonInternRegistryPtr& GetYsonInternRegistry() const;
    const NObjectServer::TRequestProfilingManagerPtr& GetRequestProfilingManager() const;
    const NChunkServer::TChunkManagerPtr& GetChunkManager() const;
    const NJournalServer::TJournalManagerPtr& GetJournalManager() const;
    const NSecurityServer::TSecurityManagerPtr& GetSecurityManager() const;
    const NSchedulerPoolServer::TSchedulerPoolManagerPtr& GetSchedulerPoolManager() const;
    const NCellServer::ITamedCellManagerPtr& GetTamedCellManager() const;
    const NTableServer::TTableManagerPtr& GetTableManager() const;
    const NTabletServer::TTabletManagerPtr& GetTabletManager() const;
    const NChaosServer::IChaosManagerPtr& GetChaosManager() const;
    const NHiveServer::THiveManagerPtr& GetHiveManager() const;
    const NHiveClient::TCellDirectoryPtr& GetCellDirectory() const;
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

private:
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
    TConfigManagerPtr ConfigManager_;
    TMulticellManagerPtr MulticellManager_;
    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NApi::NNative::IConnectionPtr ClusterConnection_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr HttpServer_;
    NElection::TCellManagerPtr CellManager_;
    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    NHydra::ISnapshotStorePtr SnapshotStore_;
    NNodeTrackerServer::TNodeTrackerPtr NodeTracker_;
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
    THydraFacadePtr HydraFacade_;
    TEpochHistoryManagerPtr EpochHistoryManager_;
    TWorldInitializerPtr WorldInitializer_;
    NObjectServer::TObjectManagerPtr ObjectManager_;
    NObjectServer::IYsonInternRegistryPtr YsonInternRegistry_;
    NObjectServer::TRequestProfilingManagerPtr RequestProfilingManager_;
    NChunkServer::TChunkManagerPtr ChunkManager_;
    NChunkServer::TChunkMergerPtr ChunkMerger_;
    NJournalServer::TJournalManagerPtr JournalManager_;
    NSecurityServer::TSecurityManagerPtr SecurityManager_;
    NCellServer::ITamedCellManagerPtr TamedCellManager_;
    NCellServer::TCellHydraJanitorPtr CellHydraJanitor_;
    NTableServer::TTableManagerPtr TableManager_;
    NTabletServer::TTabletManagerPtr TabletManager_;
    NSchedulerPoolServer::TSchedulerPoolManagerPtr SchedulerPoolManager_;
    NTabletServer::TReplicatedTableTrackerPtr ReplicatedTableTracker_;
    NChaosServer::IChaosManagerPtr ChaosManager_;
    NHiveServer::THiveManagerPtr HiveManager_;
    NHiveClient::TCellDirectoryPtr CellDirectory_;
    NHiveServer::TCellDirectorySynchronizerPtr CellDirectorySynchronizer_;
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
