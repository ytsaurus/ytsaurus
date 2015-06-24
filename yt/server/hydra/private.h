#pragma once

#include "public.h"

#include <core/misc/lazy_ptr.h>

#include <ytlib/hydra/private.h>

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

extern const Stroka SnapshotExtension;
extern const Stroka ChangelogExtension;
extern const Stroka ChangelogIndexExtension;

IInvokerPtr GetHydraIOInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
