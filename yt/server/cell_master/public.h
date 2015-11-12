#pragma once

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMasterCellConfig)
DECLARE_REFCOUNTED_CLASS(TMasterHydraManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellMasterConfig)

DECLARE_REFCOUNTED_CLASS(TMasterAutomaton)
DECLARE_REFCOUNTED_CLASS(TMasterAutomatonPart)
DECLARE_REFCOUNTED_CLASS(THydraFacade)
DECLARE_REFCOUNTED_CLASS(TWorldInitializer)

class TBootstrap;

class TLoadContext;
class TSaveContext;
typedef TCustomPersistenceContext<TSaveContext, TLoadContext> TPersistenceContext;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Mutation)
    (ChunkMaintenance)
    (ChunkLocator)
    (ChunkTraverser)
    (FullHeartbeat)
    (IncrementalHeartbeat)
    (RpcService)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
