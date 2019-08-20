#pragma once

#include <yt/core/misc/enum.h>
#include <yt/core/misc/public.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClusterClockConfig)
DECLARE_REFCOUNTED_CLASS(TClockHydraManagerConfig)

DECLARE_REFCOUNTED_CLASS(TClockAutomaton)
DECLARE_REFCOUNTED_CLASS(TClockAutomatonPart)
DECLARE_REFCOUNTED_CLASS(THydraFacade)

class TBootstrap;

enum class EClockSnapshotVersion;
class TLoadContext;
class TSaveContext;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext, EClockSnapshotVersion>;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Periodic)
    (Mutation)
    (TimestampManager)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
