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

DECLARE_REFCOUNTED_CLASS(TMasterHydraManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryServersConfig)
DECLARE_REFCOUNTED_CLASS(TMulticellManagerConfig)
DECLARE_REFCOUNTED_CLASS(TWorldInitializerConfig)
DECLARE_REFCOUNTED_CLASS(TTestConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicMulticellManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellMasterConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicCellMasterConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicQueueAgentServerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicClusterConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicResponseKeeperConfig)

DECLARE_REFCOUNTED_CLASS(TMasterAutomaton)
DECLARE_REFCOUNTED_CLASS(TMasterAutomatonPart)
DECLARE_REFCOUNTED_CLASS(TMultiPhaseCellSyncSession)
DECLARE_REFCOUNTED_CLASS(TDiskSpaceProfiler)
DECLARE_REFCOUNTED_CLASS(TMultiPhaseCellSyncSession)
DECLARE_REFCOUNTED_CLASS(TMultiPhaseCellSyncSession)
DECLARE_REFCOUNTED_CLASS(TDiskSpaceProfiler)

DECLARE_REFCOUNTED_STRUCT(IAlertManager)
DECLARE_REFCOUNTED_STRUCT(IConfigManager)
DECLARE_REFCOUNTED_STRUCT(IEpochHistoryManager)
DECLARE_REFCOUNTED_STRUCT(IResponseKeeperManager)
DECLARE_REFCOUNTED_STRUCT(IHydraFacade)
DECLARE_REFCOUNTED_STRUCT(IMulticellManager)
DECLARE_REFCOUNTED_STRUCT(IWorldInitializer)

class TBootstrap;

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
    (Zookeeper)
    (ChunkReincarnator)
    (GraftingManager)
    (NodeStatisticsFixer)
    (MaintenanceTracker)
    (MasterCellChunkStatisticsCollector)
    (CypressTransactionService)
    (TransactionService)
    (LeaseManager)
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
