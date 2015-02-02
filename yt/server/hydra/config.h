#pragma once

#include "public.h"

#include <core/misc/config.h>

#include <core/compression/public.h>

#include <core/ytree/yson_serializable.h>

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
            .Default((i64) 10 * 1024 * 1024);
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::MilliSeconds(50));
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
    NApi::TFileReaderConfigPtr Reader;
    NApi::TFileWriterConfigPtr Writer;

    TRemoteSnapshotStoreConfig()
    {
        RegisterParameter("reader", Reader)
            .DefaultNew();
        RegisterParameter("writer", Writer)
            .DefaultNew();
    }

};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStoreConfig)

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

class TDistributedHydraManagerConfig
    : public NElection::TElectionManagerConfig
{
public:
    //! Commit RPC requests timeout (leader-to-follower commit propagation).
    TDuration CommitRpcTimeout;

    //! Backoff time for unrecoverable errors causing restart.
    TDuration RestartBackoffTime;

    //! Maximum time allotted to construct a snapshot.
    TDuration SnapshotBuildTimeout;

    //! Maximum time interval between consequent snapshots.
    TDuration SnapshotBuildPeriod;

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
    TDuration MaxCommitBatchDelay;

    //! Maximum number of records to collect before flushing the current batch.
    int MaxCommitBatchRecordCount;

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

    TDistributedHydraManagerConfig()
    {
        RegisterParameter("commit_rpc_timeout", CommitRpcTimeout)
            .Default(TDuration::Seconds(30));

        RegisterParameter("restart_backoff_time", RestartBackoffTime)
            .Default(TDuration::Seconds(5));

        RegisterParameter("snapshot_build_timeout", SnapshotBuildTimeout)
            .Default(TDuration::Minutes(5));
        RegisterParameter("snapshot_build_period", SnapshotBuildPeriod)
            .Default(TDuration::Minutes(60));

        RegisterParameter("changelog_download_rpc_timeout", ChangelogDownloadRpcTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("max_changelog_records_per_request", MaxChangelogRecordsPerRequest)
            .GreaterThan(0)
            .Default(64 * 1024);
        RegisterParameter("max_changelog_bytes_per_request", MaxChangelogBytesPerRequest)
            .GreaterThan(0)
            .Default((i64) 128 * 1024 * 1024);

        RegisterParameter("snapshot_download_rpc_timeout", SnapshotDownloadRpcTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("snapshot_download_block_size", SnapshotDownloadBlockSize)
            .GreaterThan(0)
            .Default((i64) 32 * 1024 * 1024);

        RegisterParameter("max_commmit_batch_delay", MaxCommitBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        RegisterParameter("max_commit_batch_record_count", MaxCommitBatchRecordCount)
            .Default(10000);

        RegisterParameter("max_changelog_record_count", MaxChangelogRecordCount)
            .Default(1000000)
            .GreaterThan(0);
        RegisterParameter("max_changelog_data_size", MaxChangelogDataSize)
            .Default((i64) 1024 * 1024 * 1024)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
