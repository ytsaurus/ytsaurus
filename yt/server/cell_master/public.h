#pragma once

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCellStatistics;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDynamicClusterConfig)

DECLARE_REFCOUNTED_CLASS(TMasterHydraManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TMulticellManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellMasterConfig)

DECLARE_REFCOUNTED_CLASS(TMasterAutomaton)
DECLARE_REFCOUNTED_CLASS(TMasterAutomatonPart)
DECLARE_REFCOUNTED_CLASS(THydraFacade)
DECLARE_REFCOUNTED_CLASS(TWorldInitializer)
DECLARE_REFCOUNTED_CLASS(TIdentityManager)
DECLARE_REFCOUNTED_CLASS(TMulticellManager)
DECLARE_REFCOUNTED_CLASS(TConfigManager)

class TBootstrap;

class TLoadContext;
class TSaveContext;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext>;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (TimestampManager)
    (ConfigManager)
    (ChunkManager)
    (CypressManager)
    (HiveManager)
    (JournalManager)
    (NodeTracker)
    (ObjectManager)
    (SecurityManager)
    (TransactionManager)
    (MulticellManager)
    (TabletManager)
    (ExpirationTracker)
    (TabletBalancer)
    (TabletTracker)
    (Periodic)
    (Mutation)
    (ChunkMaintenance)
    (ChunkLocator)
    (ChunkFetchingTraverser)
    (ChunkStatisticsTraverser)
    (ChunkPropertiesUpdateTraverser)
    (ChunkReplicaAllocator)
    (ChunkService)
    (CypressTraverser)
    (NodeTrackerService)
    (ObjectService)
    (TransactionSupervisor)
    (GarbageCollector)
    (JobTrackerService)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
