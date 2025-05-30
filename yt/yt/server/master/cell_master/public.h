#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/cell_master_client/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellStatistics;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMasterHydraManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectionConfig)
DECLARE_REFCOUNTED_STRUCT(TDiscoveryServersConfig)
DECLARE_REFCOUNTED_STRUCT(TMulticellManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TWorldInitializerConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterCellDirectoryConfig)
DECLARE_REFCOUNTED_STRUCT(TTestConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicMulticellManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TCellMasterProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TCellMasterBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicCellMasterConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicQueueAgentServerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicClusterConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicResponseKeeperConfig)

DECLARE_REFCOUNTED_CLASS(TMasterAutomaton)
DECLARE_REFCOUNTED_CLASS(TMasterAutomatonPart)
DECLARE_REFCOUNTED_CLASS(TMultiPhaseCellSyncSession)

DECLARE_REFCOUNTED_STRUCT(IAlertManager)
DECLARE_REFCOUNTED_STRUCT(IConfigManager)
DECLARE_REFCOUNTED_STRUCT(IEpochHistoryManager)
DECLARE_REFCOUNTED_STRUCT(IResponseKeeperManager)
DECLARE_REFCOUNTED_STRUCT(IHydraFacade)
DECLARE_REFCOUNTED_STRUCT(IMulticellManager)
DECLARE_REFCOUNTED_STRUCT(IWorldInitializer)
DECLARE_REFCOUNTED_STRUCT(IWorldInitializerCache)
DECLARE_REFCOUNTED_STRUCT(IMulticellStatisticsCollector)
DECLARE_REFCOUNTED_STRUCT(IHiveProfilingManager)

DECLARE_REFCOUNTED_CLASS(TBootstrap);

enum class EMasterReign;
class TLoadContext;
class TSaveContext;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext, EMasterReign>;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (TimestampManager)
    (ConfigManager)
    (ChunkManager)
    (CypressManager)
    (CypressService)
    (PortalManager)
    (HiveManager)
    (JournalManager)
    (NodeTracker)
    (ObjectManager)
    (SecurityManager)
    (TransactionManager)
    (MulticellManager)
    (TabletManager)
    (CypressNodeExpirationTracker)
    (ChunkExpirationTracker)
    (TabletBalancer)
    (TabletTracker)
    (TabletCellJanitor)
    (TabletGossip)
    (TabletDecommissioner)
    (SecurityGossip)
    (Periodic)
    (Mutation)
    (ChunkRefresher)
    (ChunkRequisitionUpdater)
    (ChunkSealer)
    (ChunkLocator)
    (ChunkFetchingTraverser)
    (ChunkStatisticsTraverser)
    (ChunkRequisitionUpdateTraverser)
    (ChunkReplicaAllocator)
    (ChunkService)
    (CypressTraverser)
    (NodeTrackerService)
    (NodeTrackerGossip)
    (ObjectService)
    (TransactionSupervisor)
    (GarbageCollector)
    (JobTrackerService)
    (ReplicatedTableTracker)
    (MulticellGossip)
    (ClusterDirectorySynchronizer)
    (CellDirectorySynchronizer)
    (ResponseKeeper)
    (TamedCellManager)
    (SchedulerPoolManager)
    (RecursiveResourceUsageCache)
    (ExecNodeTracker)
    (ExecNodeTrackerService)
    (CellarNodeTracker)
    (CellarNodeTrackerService)
    (TabletNodeTracker)
    (TabletNodeTrackerService)
    (DataNodeTracker)
    (DataNodeTrackerService)
    (TableManager)
    (ChunkMerger)
    (ChaosManager)
    (ChaosService)
    (AlienCellSynchronizer)
    (CellTrackerService)
    (EphemeralPtrUnref)
    (ChunkAutotomizer)
    (IncumbentManager)
    (TabletService)
    (ChunkReincarnator)
    (GraftingManager)
    (NodeStatisticsFixer)
    (MaintenanceTracker)
    (MasterCellChunkStatisticsCollector)
    (CypressTransactionService)
    (TransactionService)
    (LeaseManager)
    (GroundUpdateQueueManager)
    (CypressProxyTracker)
    (SequoiaTransactionService)
);

DEFINE_ENUM(EAutomatonThreadBucket,
    (Gossips)
    (ChunkMaintenance)
    (Transactions)
);

using NCellMasterClient::EMasterCellRole;
using NCellMasterClient::EMasterCellRoles;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
