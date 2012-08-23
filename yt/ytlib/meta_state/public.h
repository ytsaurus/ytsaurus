#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/actions/future.h>
#include <ytlib/election/common.h>
#include <ytlib/election/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaState;
typedef TIntrusivePtr<IMetaState> IMetaStatePtr;

struct TMutationContext;
struct TMutationRequest;
struct TMutationResponse;

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

struct TChangeLogDownloaderConfig;
typedef TIntrusivePtr<TChangeLogDownloaderConfig> TChangeLogDownloaderConfigPtr;

struct TSnapshotDownloaderConfig;
typedef TIntrusivePtr<TSnapshotDownloaderConfig> TSnapshotDownloaderConfigPtr;

struct TFollowerPingerConfig;
typedef TIntrusivePtr<TFollowerPingerConfig> TFollowerPingerConfigPtr;

struct TFollowerTrackerConfig;
typedef TIntrusivePtr<TFollowerTrackerConfig> TFollowerTrackerConfigPtr;

struct TLeaderCommitterConfig;
typedef TIntrusivePtr<TLeaderCommitterConfig> TLeaderCommitterConfigPtr;

struct TSnapshotBuilderConfig;
typedef TIntrusivePtr<TSnapshotBuilderConfig> TSnapshotBuilderConfigPtr;

struct TChangeLogCacheConfig;
typedef TIntrusivePtr<TChangeLogCacheConfig> TChangeLogCacheConfigPtr;

struct TSnapshotStoreConfig;
typedef TIntrusivePtr<TSnapshotStoreConfig> TSnapshotStoreConfigPtr;

struct TPersistentStateManagerConfig;
typedef TIntrusivePtr<TPersistentStateManagerConfig> TPersistentStateManagerConfigPtr;

struct TMasterDiscoveryConfig;
typedef TIntrusivePtr<TMasterDiscoveryConfig> TMasterDiscoveryConfigPtr;

struct TResponseKeeperConfig;
typedef TIntrusivePtr<TResponseKeeperConfig> TResponseKeeperConfigPtr;

class TMasterDiscovery;
typedef TIntrusivePtr<TMasterDiscovery> TMasterDiscoveryPtr;

DECLARE_ENUM(EPeerStatus,
    (Stopped)
    (Elections)
    (FollowerRecovery)
    (Following)
    (LeaderRecovery)
    (Leading)
);

// TODO(babenko): provide unique codes
DECLARE_ENUM(ECommitCode,
    ((Committed)(100))
    ((MaybeCommitted)(101))
    ((NoQuorum)(102))
    ((NoLeader)(103))
    ((ReadOnly)(104))
    ((LateMutations)(105))
    ((OutOfOrderMutations)(106))
);

using NElection::TPeerId;
using NElection::TPeerPriority;
using NElection::TEpochId;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NMetaState
} // namespace NYT
