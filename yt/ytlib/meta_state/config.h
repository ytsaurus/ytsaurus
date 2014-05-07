#pragma once

#include "public.h"

#include <ytlib/election/config.h>

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>
#include <core/rpc/retrying_channel.h>


namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TChangeLogDownloaderConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration LookupTimeout;
    TDuration ReadTimeout;
    i32 RecordsPerRequest;

    TChangeLogDownloaderConfig()
    {
        RegisterParameter("lookup_timeout", LookupTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(5));
        RegisterParameter("read_timeout", ReadTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(10));
        RegisterParameter("records_per_request", RecordsPerRequest)
            .GreaterThan(0)
            .Default(64 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotDownloaderConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration LookupTimeout;
    TDuration ReadTimeout;
    i32 BlockSize;

    TSnapshotDownloaderConfig()
    {
        RegisterParameter("lookup_timeout", LookupTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(2));
        RegisterParameter("read_timeout", ReadTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(10));
        RegisterParameter("block_size", BlockSize)
            .GreaterThan(0)
            .Default(32 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotBuilderConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration RemoteTimeout;
    TDuration LocalTimeout;

    TSnapshotBuilderConfig()
    {
        RegisterParameter("remote_timeout", RemoteTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(5));
        RegisterParameter("local_timeout", LocalTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFollowerTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration PingInterval;
    TDuration RpcTimeout;

    TFollowerTrackerConfig()
    {
        RegisterParameter("ping_interval", PingInterval)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("rpc_timeout", RpcTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLeaderCommitterConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration RpcTimeout;
    TDuration MaxBatchDelay;
    int MaxBatchSize;

    TLeaderCommitterConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(3));
        RegisterParameter("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        RegisterParameter("max_batch_size", MaxBatchSize)
            .Default(10000);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChangeLogCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    //! A path where changelogs are stored.
    Stroka Path;

    //! Maximum number of cached changelogs.
    int MaxSize;

    //! Minimum total index records size between consecutive index records.
    i64 IndexBlockSize;

    TChangeLogCacheConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("max_size", MaxSize)
            .GreaterThan(0)
            .Default(4);
        RegisterParameter("index_block_size", IndexBlockSize)
            .GreaterThan(0)
            .Default(1 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotStoreConfig
    : public NYTree::TYsonSerializable
{
public:
    //! A path where snapshots are stored.
    Stroka Path;

    //! Controls if snapshots are compressed.
    bool EnableCompression;

    TSnapshotStoreConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("enable_compression", EnableCompression)
            .Default(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeperConfig
    : public NYTree::TYsonSerializable
{
public:
    //! How long responses are kept in memory.
    TDuration ExpirationPeriod;
    //! Interval between consequent attempts to sweep expired responses.
    TDuration SweepPeriod;

    TResponseKeeperConfig()
    {
        RegisterParameter("expiration_period", ExpirationPeriod)
            .Default(TDuration::Seconds(60));
        RegisterParameter("sweep_period", SweepPeriod)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPersistentStateManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TChangeLogCacheConfigPtr ChangeLogs;
    TSnapshotStoreConfigPtr Snapshots;

    //! Snapshotting period (measured in number of changes).
    /*!
     *  This is also an upper limit for the number of records in a changelog.
     *
     *  The limit may be violated if the server is under heavy load and
     *  a new snapshot generation request is issued when the previous one is still in progress.
     *  This situation is considered abnormal and a warning is reported.
     *
     *  |Null| means that snapshot creation is switched off.
     */
    TNullable<int> MaxChangesBetweenSnapshots;

    //! Maximum number of bytes to read in a single |ReadChangeLog| request.
    i64 MaxChangeLogReadSize;

    //! Default timeout for RPC requests.
    TDuration RpcTimeout;

    NElection::TCellConfigPtr Cell;

    NElection::TElectionManagerConfigPtr Election;

    TChangeLogDownloaderConfigPtr ChangeLogDownloader;

    TSnapshotDownloaderConfigPtr SnapshotDownloader;

    TFollowerTrackerConfigPtr FollowerTracker;

    TLeaderCommitterConfigPtr LeaderCommitter;

    TSnapshotBuilderConfigPtr SnapshotBuilder;

    TChangeLogCacheConfigPtr ChangeLogCache;

    TResponseKeeperConfigPtr ResponseKeeper;

    TPersistentStateManagerConfig()
    {
        RegisterParameter("changelogs", ChangeLogs);
        RegisterParameter("snapshots", Snapshots);
        RegisterParameter("max_changes_between_snapshots", MaxChangesBetweenSnapshots)
            .Default()
            .GreaterThan(0);
        RegisterParameter("max_changelog_read_size", MaxChangeLogReadSize)
            .Default((i64) 128 * 1024 * 1024)
            .GreaterThan(0);
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::MilliSeconds(3000));
        RegisterParameter("cell", Cell)
            .DefaultNew();
        RegisterParameter("election", Election)
            .DefaultNew();
        RegisterParameter("changelog_downloader", ChangeLogDownloader)
            .DefaultNew();
        RegisterParameter("snapshot_downloader", SnapshotDownloader)
            .DefaultNew();
        RegisterParameter("follower_tracker", FollowerTracker)
            .DefaultNew();
        RegisterParameter("leader_committer", LeaderCommitter)
            .DefaultNew();
        RegisterParameter("snapshot_builder", SnapshotBuilder)
            .DefaultNew();
        RegisterParameter("change_log_cache", ChangeLogCache)
            .DefaultNew();
        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Master discovery configuration.
class TMasterDiscoveryConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! List of peer addresses.
    std::vector<Stroka> Addresses;

    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    //! Master connection priority.
    int ConnectionPriority;

    TMasterDiscoveryConfig()
    {
        RegisterParameter("addresses", Addresses)
            .NonEmpty();
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("connection_priority", ConnectionPriority)
            .InRange(0, 6)
            .Default(6);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
