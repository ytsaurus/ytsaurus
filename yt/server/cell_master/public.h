#pragma once

#include <core/misc/public.h>
#include <core/misc/enum.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMasterHydraManagerConfig)
DECLARE_REFCOUNTED_CLASS(TMulticellManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellMasterConfig)

DECLARE_REFCOUNTED_CLASS(TMasterAutomaton)
DECLARE_REFCOUNTED_CLASS(TMasterAutomatonPart)
DECLARE_REFCOUNTED_CLASS(THydraFacade)
DECLARE_REFCOUNTED_CLASS(TWorldInitializer)
DECLARE_REFCOUNTED_CLASS(TIdentityManager)
DECLARE_REFCOUNTED_CLASS(TMulticellManager)

class TBootstrap;

class TLoadContext;
class TSaveContext;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext>;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Mutation)
    (ChunkMaintenance)
    (ChunkLocator)
    (ChunkTraverser)
    (FullHeartbeat)
    (IncrementalHeartbeat)
);

DEFINE_ENUM(EExternalizationMode,
    (Disabled)
    (Automatic)
    (Manual)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
