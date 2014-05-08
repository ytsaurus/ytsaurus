#pragma once

#include "public.h"

#include <core/compression/public.h>

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

#include <ytlib/api/config.h>

#include <server/election/config.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogConfig
    : public TYsonSerializable
{
public:
    //! Minimum total index records size between consecutive index records.
    i64 IndexBlockSize;

    //! Bytes to keep in memory before doing flush.
    i64 FlushBufferSize;

    //! Interval between consequent forced flushes.
    TDuration FlushPeriod;

    TFileChangelogConfig()
    {
        RegisterParameter("index_block_size", IndexBlockSize)
            .GreaterThan(0)
            .Default((i64) 128 * 1024 * 1024);
        RegisterParameter("append_buffer_size", FlushBufferSize)
            .GreaterThanOrEqual(0)
            .Default(0);
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::MilliSeconds(1000));
    }
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogConfig)

class TFileChangelogStoreConfig
    : public TFileChangelogConfig
{
public:
    //! A path where changelogs are stored.
    Stroka Path;

    //! Maximum number of cached changelogs.
    int MaxCachedChangelogs;

    TFileChangelogStoreConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("max_cached_changelogs", MaxCachedChangelogs)
            .GreaterThan(0)
            .Default(4);
    }
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogStoreConfig)

class TMultiplexedFileChangelogConfig
    : public TFileChangelogConfig
{
public:
    //! Changelog record count limit.
    /*!
     *  When this limit is reached, the current multiplexed changelog is rotated.
     */
    int MaxChangelogRecordCount;

    //! Changelog data size limit, in bytes.
    /*!
     *  See #MaxChangelogRecordCount.
     */
    i64 MaxChangelogDataSize;

    TMultiplexedFileChangelogConfig()
    {
        RegisterParameter("max_changelog_record_count", MaxChangelogRecordCount)
            .Default(1000000)
            .GreaterThan(0);
        RegisterParameter("max_changelog_data_size", MaxChangelogDataSize)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiplexedFileChangelogConfig)

class TFileChangelogCatalogConfig
    : public TYsonSerializable
{
public:
    //! A path where changelogs are stored.
    Stroka Path;

    //! Multiplexed changelogs configuration.
    TMultiplexedFileChangelogConfigPtr Multiplexed;

    //! Split changelogs configuration.
    TFileChangelogConfigPtr Split;

    //! Maximum number of cached split changelogs.
    int MaxCachedChangelogs;

    //! Maximum bytes of multiplexed changelog to read during
    //! a single iteration of replay.
    i64 ReplayBufferSize;

    TFileChangelogCatalogConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("multiplexed", Multiplexed)
            .DefaultNew();
        RegisterParameter("split", Split)
            .DefaultNew();
        RegisterParameter("max_cached_changelogs", MaxCachedChangelogs)
            .GreaterThan(0)
            .Default(256);
        RegisterParameter("replay_buffer_size", ReplayBufferSize)
            .GreaterThan(0)
            .Default(256 * 1024 * 1024);

        RegisterInitializer([&] () {
            // Expect many splits -- adjust configuration.
            Split->FlushBufferSize = (i64) 16 * 1024 * 1024;
            Split->FlushPeriod = TDuration::Seconds(15);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogCatalogConfig)

class TLocalSnapshotStoreConfig
    : public TYsonSerializable
{
public:
    //! A path where snapshots are stored.
    Stroka Path;

    //! Codec used to write snapshots.
    NCompression::ECodec Codec;

    TLocalSnapshotStoreConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("codec", Codec)
            .Default(NCompression::ECodec::Lz4);
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalSnapshotStoreConfig)

class TRemoteSnapshotStoreConfig
    : public TYsonSerializable
{
public:
    //! A temporary path where snapshots are written before being uploaded.
    Stroka TempPath;

    NApi::TFileReaderConfigPtr Reader;
    NApi::TFileWriterConfigPtr Writer;

    TRemoteSnapshotStoreConfig()
    {
        RegisterParameter("temp_path", TempPath)
            .NonEmpty()
            .Default("/tmp/yt/hydra/snapshots");
        RegisterParameter("reader", Reader)
            .DefaultNew();
        RegisterParameter("writer", Writer)
            .DefaultNew();
    }

};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStoreConfig)

class TSnapshotDownloaderConfig
    : public TYsonSerializable
{
public:
    TDuration RpcTimeout;
    i64 BlockSize;

    TSnapshotDownloaderConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("block_size", BlockSize)
            .GreaterThan(0)
            .Default((i64) 32 * 1024 * 1024);
    }
};

DEFINE_REFCOUNTED_TYPE(TSnapshotDownloaderConfig)

class TChangelogDownloaderConfig
    : public TYsonSerializable
{
public:
    TDuration RpcTimeout;
    int RecordsPerRequest;

    TChangelogDownloaderConfig()
    {
        RegisterParameter("read_timeout", RpcTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("records_per_request", RecordsPerRequest)
            .GreaterThan(0)
            .Default(64 * 1024);
    }
};

DEFINE_REFCOUNTED_TYPE(TChangelogDownloaderConfig)

class TFollowerTrackerConfig
    : public TYsonSerializable
{
public:
    TDuration PingInterval;
    TDuration RpcTimeout;

    TFollowerTrackerConfig()
    {
        RegisterParameter("ping_interval", PingInterval)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::MilliSeconds(1000));
    }
};

DEFINE_REFCOUNTED_TYPE(TFollowerTrackerConfig)

class TLeaderCommitterConfig
    : public TYsonSerializable
{
public:
    TDuration MaxBatchDelay;
    int MaxBatchSize;

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

    TLeaderCommitterConfig()
    {
        RegisterParameter("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        RegisterParameter("max_batch_size", MaxBatchSize)
            .Default(10000);
        RegisterParameter("max_changelog_record_count", MaxChangelogRecordCount)
            .Default(1000000)
            .GreaterThan(0);
        RegisterParameter("max_changelog_data_size", MaxChangelogDataSize)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TLeaderCommitterConfig)

class TDistributedHydraManagerConfig
    : public TYsonSerializable
{
public:
    //! Default timeout for control RPC requests.
    TDuration RpcTimeout;

    //! Maximum time allotted to construct a snapshot.
    TDuration SnapshotTimeout;

    //! Backoff time for unrecoverable internal errors.
    TDuration BackoffTime;

    //! Should we build snapshots at followers?
    bool BuildSnapshotsAtFollowers;

    //! Maximum number of bytes to read from a changelog at once.
    i64 MaxChangelogReadSize;

    NElection::TElectionManagerConfigPtr Elections;

    TChangelogDownloaderConfigPtr ChangelogDownloader;

    TSnapshotDownloaderConfigPtr SnapshotDownloader;

    TFollowerTrackerConfigPtr FollowerTracker;

    TLeaderCommitterConfigPtr LeaderCommitter;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TDistributedHydraManagerConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("snapshot_timeout", SnapshotTimeout)
            .Default(TDuration::Minutes(5));
        RegisterParameter("backoff_time", BackoffTime)
            .Default(TDuration::Seconds(5));
        RegisterParameter("build_snapshots_at_followers", BuildSnapshotsAtFollowers)
            .Default(true);
        RegisterParameter("max_changelog_read_size", MaxChangelogReadSize)
            .Default((i64) 128 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("elections", Elections)
            .DefaultNew();
        RegisterParameter("changelog_downloader", ChangelogDownloader)
            .DefaultNew();
        RegisterParameter("snapshot_downloader", SnapshotDownloader)
            .DefaultNew();
        RegisterParameter("follower_tracker", FollowerTracker)
            .DefaultNew();
        RegisterParameter("leader_committer", LeaderCommitter)
            .DefaultNew();
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
