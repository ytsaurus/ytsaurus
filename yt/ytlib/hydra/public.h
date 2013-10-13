#pragma once

#include <core/misc/common.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TPeerDiscoveryConfig;
typedef TIntrusivePtr<TPeerDiscoveryConfig> TPeerDiscoveryConfigPtr;

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TMutationId;
extern const TMutationId NullMutationId;

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellGuid;
using NElection::TPeerId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

DECLARE_ENUM(EPeerState,
    ((None)                       (0))
    ((Initializing)               (1))
    ((Stopped)                    (2))
    ((Elections)                  (3))
    ((FollowerRecovery)           (4))
    ((Following)                  (5))
    ((LeaderRecovery)             (6))
    ((Leading)                    (7))
    ((Finalizing)                 (8))
);

DECLARE_ENUM(EErrorCode,
    ((NoSuchSnapshot)             (600))
    ((NoSuchChangelog)            (601))
    ((InvalidEpoch)               (602))
    ((InvalidVersion)             (603))
    ((InvalidState)               (604))
    ((MaybeCommitted)             (605))
    ((NoQuorum)                   (606))
    ((NoLeader)                   (607))
    ((ReadOnly)                   (608))
    ((OutOfOrderMutations)        (609))
);

DECLARE_ENUM(EPeerRole,
    (Any)
    (Leader)
    (Follower)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
