#pragma once

#include <core/misc/common.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaState;
typedef TIntrusivePtr<IMetaState> IMetaStatePtr;

struct TMutationRequest;
struct TMutationResponse;
class TMutationContext;

typedef TGuid TMutationId;
extern TMutationId NullMutationId;

struct IMetaStateManager;
typedef TIntrusivePtr<IMetaStateManager> IMetaStateManagerPtr;

class TCompositeMetaState;
typedef TIntrusivePtr<TCompositeMetaState> TCompositeMetaStatePtr;

class TMetaStatePart;
typedef TIntrusivePtr<TMetaStatePart> TMetaStatePartPtr;

class TMutation;
typedef TIntrusivePtr<TMutation> TMutationPtr;

struct TMetaVersion;

class TChangeLogDownloaderConfig;
typedef TIntrusivePtr<TChangeLogDownloaderConfig> TChangeLogDownloaderConfigPtr;

class TSnapshotDownloaderConfig;
typedef TIntrusivePtr<TSnapshotDownloaderConfig> TSnapshotDownloaderConfigPtr;

class TFollowerTrackerConfig;
typedef TIntrusivePtr<TFollowerTrackerConfig> TFollowerTrackerConfigPtr;

class TLeaderCommitterConfig;
typedef TIntrusivePtr<TLeaderCommitterConfig> TLeaderCommitterConfigPtr;

class TSnapshotBuilderConfig;
typedef TIntrusivePtr<TSnapshotBuilderConfig> TSnapshotBuilderConfigPtr;

class TChangeLogCacheConfig;
typedef TIntrusivePtr<TChangeLogCacheConfig> TChangeLogCacheConfigPtr;

class TSnapshotStoreConfig;
typedef TIntrusivePtr<TSnapshotStoreConfig> TSnapshotStoreConfigPtr;

class TPersistentStateManagerConfig;
typedef TIntrusivePtr<TPersistentStateManagerConfig> TPersistentStateManagerConfigPtr;

class TMasterDiscoveryConfig;
typedef TIntrusivePtr<TMasterDiscoveryConfig> TMasterDiscoveryConfigPtr;

class TResponseKeeperConfig;
typedef TIntrusivePtr<TResponseKeeperConfig> TResponseKeeperConfigPtr;

class TMasterDiscovery;
typedef TIntrusivePtr<TMasterDiscovery> TMasterDiscoveryPtr;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EPeerStatus,
    (Stopped)
    (Elections)
    (FollowerRecovery)
    (Following)
    (LeaderRecovery)
    (Leading)
);

DECLARE_ENUM(EErrorCode,
    ((NoSuchSnapshot)             (600))
    ((NoSuchChangeLog)            (601))
    ((InvalidEpoch)               (602))
    ((InvalidVersion)             (603))
    ((InvalidStatus)              (604))
    ((SnapshotAlreadyInProgress)  (605))
    ((MaybeCommitted)             (606))
    ((NoQuorum)                   (607))
    ((NoLeader)                   (608))
    ((ReadOnly)                   (609))
    ((LateMutations)              (610))
    ((OutOfOrderMutations)        (611))
);

////////////////////////////////////////////////////////////////////////////////

using NElection::TPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
