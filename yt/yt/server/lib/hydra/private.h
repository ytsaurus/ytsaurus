#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TEpochContext)

struct TRemoteSnapshotParams;

DECLARE_REFCOUNTED_CLASS(TSyncFileChangelog)
DECLARE_REFCOUNTED_CLASS(TDecoratedAutomaton)
DECLARE_REFCOUNTED_CLASS(TLeaderRecovery)
DECLARE_REFCOUNTED_CLASS(TFollowerRecovery)
DECLARE_REFCOUNTED_CLASS(TLeaderLease)
DECLARE_REFCOUNTED_CLASS(TLeaseTracker)
DECLARE_REFCOUNTED_CLASS(TLeaderCommitter)
DECLARE_REFCOUNTED_CLASS(TFollowerCommitter)
DECLARE_REFCOUNTED_CLASS(TCheckpointer)
DECLARE_REFCOUNTED_CLASS(TStateHashChecker)

DECLARE_REFCOUNTED_STRUCT(IChangelogDiscarder)

////////////////////////////////////////////////////////////////////////////////

extern const TString SnapshotExtension;
extern const TString ChangelogExtension;
extern const TString ChangelogIndexExtension;
extern const NProfiling::TProfiler HydraProfiler;

IInvokerPtr GetHydraIOInvoker();
void ShutdownHydraIOInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
