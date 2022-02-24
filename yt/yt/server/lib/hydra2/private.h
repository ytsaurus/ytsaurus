#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDecoratedAutomaton)
DECLARE_REFCOUNTED_CLASS(TRecovery)
DECLARE_REFCOUNTED_CLASS(TLeaderLease)
DECLARE_REFCOUNTED_CLASS(TLeaseTracker)
DECLARE_REFCOUNTED_CLASS(TLeaderCommitter)
DECLARE_REFCOUNTED_CLASS(TFollowerCommitter)

DECLARE_REFCOUNTED_STRUCT(TEpochContext)
DECLARE_REFCOUNTED_STRUCT(IChangelogDiscarder)
DECLARE_REFCOUNTED_STRUCT(TPendingMutation)

////////////////////////////////////////////////////////////////////////////////

extern NConcurrency::TFls<NElection::TEpochId> CurrentEpochId;

class TCurrentEpochIdGuard
{
public:
    TCurrentEpochIdGuard(const TCurrentEpochIdGuard&) = delete;
    TCurrentEpochIdGuard(TCurrentEpochIdGuard&&) = delete;

    explicit TCurrentEpochIdGuard(NElection::TEpochId epochId);
    ~TCurrentEpochIdGuard();
};

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;
using NElection::TPeerId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

using NHydra::EPeerState;
using NHydra::EErrorCode;
using NHydra::EFinalRecoveryAction;

using NHydra::HydraLogger;

using NHydra::TVersion;
using NHydra::TElectionPriority;
using NHydra::TReachableState;

using NHydra::InvalidPeerId;
using NHydra::TReign;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
