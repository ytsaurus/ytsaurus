#pragma once

#include "public.h"

#include <yt/ytlib/hydra/private.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NHydra {

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

////////////////////////////////////////////////////////////////////////////////

extern const TString SnapshotExtension;
extern const TString ChangelogExtension;
extern const TString ChangelogIndexExtension;

IInvokerPtr GetHydraIOInvoker();
void ShutdownHydraIOInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
