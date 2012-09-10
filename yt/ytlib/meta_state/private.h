#pragma once

#include "public.h"

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TChangeLog;
typedef TIntrusivePtr<TChangeLog> TChangeLogPtr;

class TAsyncChangeLog;

class TCachedAsyncChangeLog;
typedef TIntrusivePtr<TCachedAsyncChangeLog> TCachedAsyncChangeLogPtr;

class TDecoratedMetaState;
typedef TIntrusivePtr<TDecoratedMetaState> TDecoratedMetaStatePtr;

class TFollowerTracker;
typedef TIntrusivePtr<TFollowerTracker> TFollowerTrackerPtr;

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

class TResponseKeeper;
typedef TIntrusivePtr<TResponseKeeper> TResponseKeeperPtr;

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger MetaStateLogger;
extern NProfiling::TProfiler MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

//! A special value indicating that the number of records in the previous
//! changelog is undetermined since there is no previous changelog.
/*!
 *  \see TRecovery
 */
const i32 NonexistingPrevRecordCount = -1;

//! A special value indicating that the number of records in the previous changelog
//! is unknown.
/*!
 *  \see TRecovery
 */
const i32 UnknownPrevRecordCount = -2;

//! A special value indicating that no snapshot id is known.
//! is unknown.
/*!
 *  \see TSnapshotStore
 */
const i32 NonexistingSnapshotId = -1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
