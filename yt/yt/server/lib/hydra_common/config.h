#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Minimum total index records size between consecutive index records.
    i64 IndexBlockSize;

    //! When the number of unflushed bytes exceeds this value, an automatic flush is performed.
    i64 FlushBufferSize;

    //! Interval between consequent automatic flushes.
    TDuration FlushPeriod;

    //! When |false|, no |fdatasync| calls are actually made.
    //! Should only be used in tests and local mode.
    bool EnableSync;

    //! If set, enables preallocating changelog data file to avoid excessive FS metadata
    //! (in particular, file size) updates.
    std::optional<i64> PreallocateSize;

    TFileChangelogConfig();
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogConfig)

class TFileChangelogDispatcherConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    int IOClass;
    int IOPriority;
    TDuration FlushQuantum;

    TFileChangelogDispatcherConfig();
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogDispatcherConfig)

class TFileChangelogStoreConfig
    : public TFileChangelogConfig
    , public TFileChangelogDispatcherConfig
{
public:
    //! A path where changelogs are stored.
    TString Path;

    //! Maximum number of cached changelogs.
    TSlruCacheConfigPtr ChangelogReaderCache;

    NIO::EIOEngineType IOEngineType;
    NYTree::INodePtr IOConfig;

    TFileChangelogStoreConfig();
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogStoreConfig)

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotStoreConfig
    : public NYTree::TYsonSerializable
{
public:
    //! A path where snapshots are stored.
    TString Path;

    //! Codec used to write snapshots.
    NCompression::ECodec Codec;

    TLocalSnapshotStoreConfig();
};

DEFINE_REFCOUNTED_TYPE(TLocalSnapshotStoreConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteSnapshotStoreConfig
    : public NYTree::TYsonSerializable
{
public:
    NApi::TFileReaderConfigPtr Reader;
    NApi::TFileWriterConfigPtr Writer;

    TRemoteSnapshotStoreConfig();
};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStoreConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStoreConfig
    : public NYTree::TYsonSerializable
{
public:
    NApi::TJournalReaderConfigPtr Reader;
    NApi::TJournalWriterConfigPtr Writer;
    std::optional<TDuration> LockTransactionTimeout;

    TRemoteChangelogStoreConfig();
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreConfig)

////////////////////////////////////////////////////////////////////////////////

class THydraJanitorConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<int> MaxSnapshotCountToKeep;
    std::optional<i64> MaxSnapshotSizeToKeep;
    std::optional<int> MaxChangelogCountToKeep;
    std::optional<i64> MaxChangelogSizeToKeep;

    THydraJanitorConfig();
};

DEFINE_REFCOUNTED_TYPE(THydraJanitorConfig)

////////////////////////////////////////////////////////////////////////////////

class TLocalHydraJanitorConfig
    : public THydraJanitorConfig
{
public:
    TDuration CleanupPeriod;

    TLocalHydraJanitorConfig();
};

DEFINE_REFCOUNTED_TYPE(TLocalHydraJanitorConfig)

////////////////////////////////////////////////////////////////////////////////

class TDistributedHydraManagerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Timeout for various control RPC requests.
    TDuration ControlRpcTimeout;

    //! The maximum time interval mutations are allowed to occupy the automaton thread
    //! before yielding control to other callbacks.
    TDuration MaxCommitBatchDuration;

    //! Interval between consequent lease lease checks.
    TDuration LeaderLeaseCheckPeriod;

    //! Timeout after which leader lease expires.
    TDuration LeaderLeaseTimeout;

    //! Time a newly elected leader waits before becoming active.
    TDuration LeaderLeaseGraceDelay;

    //! When set to |true|, disables leader grace delay.
    //! For tests only!
    bool DisableLeaderLeaseGraceDelay;

    //! Leader-to-follower commit timeout.
    TDuration CommitFlushRpcTimeout;

    //! Follower-to-leader commit forwarding timeout.
    TDuration CommitForwardingRpcTimeout;

    //! Backoff time for unrecoverable errors causing restart.
    TDuration RestartBackoffTime;

    //! Maximum time allotted to construct a snapshot.
    TDuration SnapshotBuildTimeout;

    //! Maximum time allotted to fork during snapshot building.
    //! If process did not fork within this timeout, it crashes.
    TDuration SnapshotForkTimeout;

    //! Maximum time interval between consequent snapshots.
    TDuration SnapshotBuildPeriod;

    //! Random splay for snapshot building.
    TDuration SnapshotBuildSplay;

    //! Generic timeout for RPC calls during changelog download.
    TDuration ChangelogDownloadRpcTimeout;

    //! Maximum number of bytes to read from a changelog at once.
    i64 MaxChangelogBytesPerRequest;

    //! Maximum number of records to read from a changelog at once.
    int MaxChangelogRecordsPerRequest;

    //! Generic timeout for RPC calls during snapshot download.
    TDuration SnapshotDownloadRpcTimeout;

    //! Block size used during snapshot download.
    i64 SnapshotDownloadBlockSize;

    //! Maximum time to wait before flushing the current batch.
    // COMPAT(babenko): no longer used in Hydra2.
    TDuration MaxCommitBatchDelay;

    //! Maximum number of records to collect before flushing the current batch.
    int MaxCommitBatchRecordCount;

    //! The period between consecutive serializations, i.e. moving
    //! mutations from from draft queue to mutation queue and thus assigning sequence numbers.
    TDuration MutationSerializationPeriod;

    //! The period between consecutive flushes, i.e. sending mutations
    //! from a leader to its followers.
    TDuration MutationFlushPeriod;

    //! Maximum time to wait before syncing with leader.
    TDuration LeaderSyncDelay;

    //! Changelog record count limit.
    /*!
     *  When this limit is reached, the current changelog is rotated and a snapshot
     *  is built.
     */
    int MaxChangelogRecordCount;

    //! Changelog data size limit, in bytes.
    /*!
     *  See #MaxChangelogRecordCount.
     */
    i64 MaxChangelogDataSize;

    //! If true, empty changelogs are preallocated to avoid hiccups of segment rotation.
    // COMPAT(babenko): no longer used in Hydra2
    bool PreallocateChangelogs;

    //! If true, changelogs are gracefully closed on segment rotation and epoch end.
    // COMPAT(babenko): no longer used in Hydra2.
    bool CloseChangelogs;

    //! Interval between automatic "heartbeat" mutations commit.
    /*!
     *  These mutations are no-ops. Committing them regularly helps to ensure
     *  that the quorum is functioning properly.
     */
    TDuration HeartbeatMutationPeriod;

    //! If "heartbeat" mutation commit takes longer than this value, Hydra is restarted.
    TDuration HeartbeatMutationTimeout;

    //! Period for retrying while waiting for changelog record count to become
    //! sufficiently high to proceed with applying mutations.
    TDuration ChangelogRecordCountCheckRetryPeriod;

    //! If mutation logging remains suspended for this period of time,
    //! Hydra restarts.
    TDuration MutationLoggingSuspensionTimeout;

    //! Time to sleep before building a snapshot. Needed for testing.
    TDuration BuildSnapshotDelay;

    //! Persistent stores initialization has exponential retries.
    //! Minimum persistent store initializing backoff time.
    TDuration MinPersistentStoreInitializationBackoffTime;

    //! Maximum persistent store initializing backoff time.
    TDuration MaxPersistentStoreInitializationBackoffTime;

    //! Persistent store initializing backoff time multiplier.
    double PersistentStoreInitializationBackoffTimeMultiplier;

    //! Abandon leader lease request timeout.
    TDuration AbandonLeaderLeaseRequestTimeout;

    //! Enables logging in mutation handlers even during recovery.
    bool ForceMutationLogging;

    //! Enables state hash checker.
    //! It checks that after applying each N-th mutation, automaton state hash is the same on all peers.
    bool EnableStateHashChecker;

    //! Maximum number of entries stored in state hash checker.
    int MaxStateHashCheckerEntryCount;

    //! Followers will report leader every "StateHashCheckerMutationVerificationSamplingRate"-th mutation's state hash.
    int StateHashCheckerMutationVerificationSamplingRate;

    //! In case Hydra leader is not restarted after switch has been initiated within this timeout,
    //! it will restart automatically.
    TDuration LeaderSwitchTimeout;

    //! Maximum amount of mutations stored in leader's mutation queue.
    int MaxQueuedMutationCount;

    //! Leader's mutation queue data size limit, in bytes.
    int MaxQueuedMutationDataSize;

    //! If set, automaton invariants are checked after each mutation with this probability.
    //! Used for testing purposes only.
    std::optional<double> InvariantsCheckProbability;

    TDistributedHydraManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDistributedHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
