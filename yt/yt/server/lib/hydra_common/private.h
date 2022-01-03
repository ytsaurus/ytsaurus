#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteSnapshotParams;

DECLARE_REFCOUNTED_CLASS(TStateHashChecker)

////////////////////////////////////////////////////////////////////////////////

extern const TString SnapshotExtension;
extern const TString ChangelogExtension;
extern const TString ChangelogIndexExtension;

inline const NProfiling::TProfiler HydraProfiler("/hydra");

IInvokerPtr GetHydraIOInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
