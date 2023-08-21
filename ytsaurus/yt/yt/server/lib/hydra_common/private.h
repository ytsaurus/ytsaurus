#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteSnapshotParams;

DECLARE_REFCOUNTED_CLASS(TStateHashChecker)
DECLARE_REFCOUNTED_CLASS(TFileChangelogIndex)

DECLARE_REFCOUNTED_STRUCT(IUnbufferedFileChangelog)

////////////////////////////////////////////////////////////////////////////////

inline const TString SnapshotExtension("snapshot");
inline const TString ChangelogExtension("log");
inline const TString ChangelogIndexExtension("index");
inline const TString TermFileName("term");
inline const TString LockFileName("lock");

inline const NProfiling::TProfiler HydraProfiler("/hydra");

// TODO(babenko): drop once Hydra1 is no more.
IInvokerPtr GetHydraIOInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
