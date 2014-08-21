#pragma once

#include "public.h"

#include <core/misc/config.h>

#include <core/compression/public.h>

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

#include <ytlib/api/config.h>

#include <server/election/config.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TFileChangelogConfig
    : public NYTree::TYsonSerializable
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
            .Default((i64) 1024 * 1024);
        RegisterParameter("flush_buffer_size", FlushBufferSize)
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
    TSlruCacheConfigPtr ChangelogReaderCache;

    TFileChangelogStoreConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("changelog_reader_cache", ChangelogReaderCache)
            .DefaultNew();

        RegisterInitializer([&] () {
           ChangelogReaderCache->Capacity = 4;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogStoreConfig)

class TLocalSnapshotStoreConfig
    : public NYTree::TYsonSerializable
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
    : public NYTree::TYsonSerializable
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
    : public NYTree::TYsonSerializable
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

class TRemoteChangelogStoreConfig
    : public NYTree::TYsonSerializable
{
public:
    NApi::TJournalReaderConfigPtr Reader;
    NApi::TJournalWriterConfigPtr Writer;

    TRemoteChangelogStoreConfig()
    {
        RegisterParameter("reader", Reader)
            .DefaultNew();
        RegisterParameter("writer", Writer)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreConfig)

class TChangelogDownloaderConfig
    : public NYTree::TYsonSerializable
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
    : public NYTree::TYsonSerializable
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
    : public NYTree::TYsonSerializable
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

    //! Maximum time interval between consequent snapshots.
    TDuration AutoSnapshotPeriod;


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
        RegisterParameter("auto_snapshot_period", AutoSnapshotPeriod)
            .Default(TDuration::Minutes(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TLeaderCommitterConfig)

class TDistributedHydraManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Default timeout for control RPC requests.
    TDuration RpcTimeout;

    //! Maximum time allotted to construct a snapshot.
    TDuration SnapshotTimeout;

    //! Backoff time for unrecoverable internal errors.
    TDuration BackoffTime;

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
