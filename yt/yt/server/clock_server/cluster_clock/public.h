#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TClusterClockBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TClusterProgramClockConfig)
DECLARE_REFCOUNTED_STRUCT(TClockHydraManagerConfig)

DECLARE_REFCOUNTED_CLASS(TClockAutomaton)
DECLARE_REFCOUNTED_CLASS(TClockAutomatonPart)
DECLARE_REFCOUNTED_CLASS(THydraFacade)
DECLARE_REFCOUNTED_CLASS(TBootstrap)

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
