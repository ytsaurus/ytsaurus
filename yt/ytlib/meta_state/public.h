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

struct IMetaStateManager;
typedef TIntrusivePtr<IMetaStateManager> IMetaStateManagerPtr;

class TChangeLog;
typedef TIntrusivePtr<TChangeLog> TChangeLogPtr;

class TAsyncChangeLog;

class TCachedAsyncChangeLog;
typedef TIntrusivePtr<TCachedAsyncChangeLog> TCachedAsyncChangeLogPtr;

class TCompositeMetaState;
typedef TIntrusivePtr<TCompositeMetaState> TCompositeMetaStatePtr;

class TMetaStatePart;
typedef TIntrusivePtr<TMetaStatePart> TMetaStatePartPtr;

class TDecoratedMetaState;
typedef TIntrusivePtr<TDecoratedMetaState> TDecoratedMetaStatePtr;

class TFollowerTracker;
typedef TIntrusivePtr<TFollowerTracker> TFollowerTrackerPtr;

class TFollowerPinger;
typedef TIntrusivePtr<TFollowerPinger> TFollowerPingerPtr;

template <class TResult>
class TMutation;

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

class TRecovery;
typedef TIntrusivePtr<TRecovery> TRecoveryPtr;

class TLeaderRecovery;
typedef TIntrusivePtr<TLeaderRecovery> TLeaderRecoveryPtr;

class TFollowerRecovery;
typedef TIntrusivePtr<TFollowerRecovery> TFollowerRecoveryPtr;

class TCommitter;
typedef TIntrusivePtr<TCommitter> TCommitterPtr;

class TLeaderCommitter;
typedef TIntrusivePtr<TLeaderCommitter> TLeaderCommitterPtr;

class TFollowerCommitter;
typedef TIntrusivePtr<TFollowerCommitter> TFollowerCommitterPtr;

class TSnapshotReader;
typedef TIntrusivePtr<TSnapshotReader> TSnapshotReaderPtr;

class TSnapshotWriter;
typedef TIntrusivePtr<TSnapshotWriter> TSnapshotWriterPtr;

class TSnapshotBuilder;
typedef TIntrusivePtr<TSnapshotBuilder> TSnapshotBuilderPtr;

class TSnapshotDownloader;

class TSnapshotLookup;

class TChangeLogCache;
typedef TIntrusivePtr<TChangeLogCache> TChangeLogCachePtr;

class TSnapshotStore;
typedef TIntrusivePtr<TSnapshotStore> TSnapshotStorePtr;

DECLARE_ENUM(EPeerStatus,
    (Stopped)
    (Elections)
    (FollowerRecovery)
    (Following)
    (LeaderRecovery)
    (Leading)
);

DECLARE_ENUM(ECommitResult,
    (Committed)
    (MaybeCommitted)
    (NotCommitted)
    (InvalidStatus)
    (ReadOnly)
);

typedef TFuture<ECommitResult> TAsyncCommitResult;

using NElection::TPeerId;
using NElection::TPeerPriority;
using NElection::TEpoch;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NMetaState
} // namespace NYT
