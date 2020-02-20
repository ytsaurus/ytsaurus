#pragma once

#include <yt/client/election/public.h>

#include <yt/core/misc/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPeerState,
    ((None)                       (0))
    ((Stopped)                    (1))
    ((Elections)                  (2))
    ((FollowerRecovery)           (3))
    ((Following)                  (4))
    ((LeaderRecovery)             (5))
    ((Leading)                    (6))
);

DEFINE_ENUM(EErrorCode,
    ((NoSuchSnapshot)              (600))
    ((NoSuchChangelog)             (601))
    ((InvalidEpoch)                (602))
    ((InvalidVersion)              (603))
    ((OutOfOrderMutations)         (609))
    ((InvalidSnapshotVersion)      (610))
    ((ReadOnlySnapshotBuilt)       (611))
    ((ReadOnlySnapshotBuildFailed) (612))
    ((BrokenChangelog)             (613))
    ((ChangelogIOError)            (614))
    ((InvalidChangelogState)       (615))
);

DEFINE_ENUM(EPeerKind,
    (Leader)
    (Follower)
    (LeaderOrFollower)
);

using TRevision = ui64;
constexpr TRevision NullRevision = 0;

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;
using NElection::TPeerId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
