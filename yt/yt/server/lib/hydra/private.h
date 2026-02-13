#pragma once

#include "public.h"

#include <yt/yt/ytlib/hydra/private.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TRemoteSnapshotParams;

DECLARE_REFCOUNTED_CLASS(TStateHashChecker)
DECLARE_REFCOUNTED_CLASS(TFileChangelogIndex)

DECLARE_REFCOUNTED_STRUCT(IUnbufferedFileChangelog)

////////////////////////////////////////////////////////////////////////////////

inline const std::string SnapshotExtension("snapshot");
inline const std::string ChangelogExtension("log");
inline const std::string ChangelogIndexExtension("index");
inline const std::string TermFileName("term");
inline const std::string LockFileName("lock");

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, HydraProfiler, "/hydra");

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
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
