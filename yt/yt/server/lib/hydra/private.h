#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/concurrency/fls.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

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

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDecoratedAutomaton)
DECLARE_REFCOUNTED_CLASS(TRecovery)
DECLARE_REFCOUNTED_CLASS(TLeaderLease)
DECLARE_REFCOUNTED_CLASS(TLeaseTracker)
DECLARE_REFCOUNTED_CLASS(TLeaderCommitter)
DECLARE_REFCOUNTED_CLASS(TFollowerCommitter)
DECLARE_REFCOUNTED_CLASS(TConfigWrapper)

DECLARE_REFCOUNTED_STRUCT(TEpochContext)
DECLARE_REFCOUNTED_STRUCT(IChangelogDiscarder)
DECLARE_REFCOUNTED_STRUCT(TPendingMutation)

////////////////////////////////////////////////////////////////////////////////

extern NConcurrency::TFlsSlot<NElection::TEpochId> CurrentEpochId;

class TCurrentEpochIdGuard
{
public:
    TCurrentEpochIdGuard(const TCurrentEpochIdGuard&) = delete;
    TCurrentEpochIdGuard(TCurrentEpochIdGuard&&) = delete;

    explicit TCurrentEpochIdGuard(NElection::TEpochId epochId);
    ~TCurrentEpochIdGuard();
};

////////////////////////////////////////////////////////////////////////////////

class TConfigWrapper
    : public TRefCounted
{
public:
    explicit TConfigWrapper(TDistributedHydraManagerConfigPtr config);

    void Set(TDistributedHydraManagerConfigPtr config);
    TDistributedHydraManagerConfigPtr Get() const;

private:
    TAtomicIntrusivePtr<TDistributedHydraManagerConfig> Config_;
};

DEFINE_REFCOUNTED_TYPE(TConfigWrapper)

////////////////////////////////////////////////////////////////////////////////

bool IsSystemMutationType(const TString& mutationType);

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;
using NElection::TPeerId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
