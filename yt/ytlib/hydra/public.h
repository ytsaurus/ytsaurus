#pragma once

#include <core/misc/common.h>

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPeerConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStoreOptions)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreOptions)

DECLARE_ENUM(EPeerState,
    ((None)                       (0))
    ((Initializing)               (1))
    ((Stopped)                    (2))
    ((Elections)                  (3))
    ((FollowerRecovery)           (4))
    ((Following)                  (5))
    ((LeaderRecovery)             (6))
    ((Leading)                    (7))
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

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellGuid;
using NElection::NullCellGuid;
using NElection::TPeerId;
using NElection::InvalidPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

using NRpc::TMutationId;
using NRpc::NullMutationId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
