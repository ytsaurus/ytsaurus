#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/actions/future.h>
#include <ytlib/election/common.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct IMetaState;
typedef TIntrusivePtr<IMetaState> IMetaStatePtr;

struct IMetaStateManager;
typedef TIntrusivePtr<IMetaStateManager> IMetaStateManagerPtr;

struct TCellConfig;
typedef TIntrusivePtr<TCellConfig> TCellConfigPtr;

class TCellManager;
typedef TIntrusivePtr<TCellManager> TCellManagerPtr;

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

template <class TResult>
class TMetaChange;

struct TMetaVersion;

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
typedef TIntrusivePtr<TSnapshotDownloader> TSnapshotDownloaderPtr;

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
