#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TEpochContext)

DECLARE_REFCOUNTED_CLASS(TDecoratedAutomaton)
DECLARE_REFCOUNTED_CLASS(TLeaderRecovery)
DECLARE_REFCOUNTED_CLASS(TFollowerRecovery)
DECLARE_REFCOUNTED_CLASS(TLeaderLease)
DECLARE_REFCOUNTED_CLASS(TLeaseTracker)
DECLARE_REFCOUNTED_CLASS(TLeaderCommitter)
DECLARE_REFCOUNTED_CLASS(TFollowerCommitter)
DECLARE_REFCOUNTED_CLASS(TCheckpointer)
DECLARE_REFCOUNTED_STRUCT(IChangelogDiscarder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
