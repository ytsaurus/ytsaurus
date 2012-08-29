#pragma once

#include "public.h"

#include <ytlib/election/config.h>
#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct TChangeLogDownloaderConfig
    : public TYsonSerializable
{
    TDuration LookupTimeout;
    TDuration ReadTimeout;
    i32 RecordsPerRequest;

    TChangeLogDownloaderConfig()
    {
        Register("lookup_timeout", LookupTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(5));
        Register("read_timeout", ReadTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(10));
        Register("records_per_request", RecordsPerRequest)
            .GreaterThan(0)
            .Default(1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotDownloaderConfig
    : public TYsonSerializable
{
    TDuration LookupTimeout;
    TDuration ReadTimeout;
    i32 BlockSize;

    TSnapshotDownloaderConfig()
    {
        Register("lookup_timeout", LookupTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(2));
        Register("read_timeout", ReadTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(10));
        Register("block_size", BlockSize)
            .GreaterThan(0)
            .Default(32 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotBuilderConfig
    : public TYsonSerializable
{
    TDuration RemoteTimeout;
    TDuration LocalTimeout;

    TSnapshotBuilderConfig()
    {
        Register("remote_timeout", RemoteTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(5));
        Register("local_timeout", LocalTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFollowerPingerConfig
    : public TYsonSerializable
{
    TDuration PingInterval;
    TDuration RpcTimeout;

    TFollowerPingerConfig()
    {
        Register("ping_interval", PingInterval)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
        Register("rpc_timeout", RpcTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(1000));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TQuorumTrackerConfig
    : public TYsonSerializable
{
    TDuration PingTimeout;

    TQuorumTrackerConfig()
    {
        Register("ping_timeout", PingTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::MilliSeconds(3000));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLeaderCommitterConfig
    : public TYsonSerializable
{
    TDuration RpcTimeout;
    TDuration MaxBatchDelay;
    int MaxBatchSize;

    TLeaderCommitterConfig()
    {
        Register("rpc_timeout", RpcTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(3));
        Register("max_batch_delay", MaxBatchDelay)
            .Default(TDuration::MilliSeconds(10));
        Register("max_batch_size", MaxBatchSize)
            .Default(10000);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TChangeLogCacheConfig
    : public TYsonSerializable
{
    //! A path where changelogs are stored.
    Stroka Path;

    //! Maximum number of cached changelogs.
    int MaxSize;

    //! Minimum total index records size between consecutive index records.
    i64 IndexBlockSize;

    TChangeLogCacheConfig()
    {
        Register("path", Path);
        Register("max_size", MaxSize)
            .GreaterThan(0)
            .Default(4);
        Register("index_block_size", IndexBlockSize)
            .GreaterThan(0)
            .Default(1 * 1024 * 1024);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotStoreConfig
    : public TYsonSerializable
{
    //! A path where snapshots are stored.
    Stroka Path;

    //! Controls if snapshots are compressed.
    bool EnableCompression;

    TSnapshotStoreConfig()
    {
        Register("path", Path);
        Register("enable_compression", EnableCompression)
            .Default(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TResponseKeeperConfig
    : public TYsonSerializable
{
    //! How long responses are kept in memory.
    TDuration ExpirationPeriod;
    //! Interval between consequent attempts to sweep expired responses.
    TDuration SweepPeriod;

    TResponseKeeperConfig()
    {
        Register("expiration_period", ExpirationPeriod)
            .Default(TDuration::Minutes(5));
        Register("sweep_period", SweepPeriod)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStateManagerConfig
    : public TYsonSerializable
{
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

    //! Default timeout for RPC requests.
    TDuration RpcTimeout;

    NElection::TCellConfigPtr Cell;

    NElection::TElectionManagerConfigPtr Election;

    TChangeLogDownloaderConfigPtr ChangeLogDownloader;

    TSnapshotDownloaderConfigPtr SnapshotDownloader;

    TFollowerPingerConfigPtr FollowerPinger;

    TQuorumTrackerConfigPtr QuorumTracker;

    TLeaderCommitterConfigPtr LeaderCommitter;

    TSnapshotBuilderConfigPtr SnapshotBuilder;

    TChangeLogCacheConfigPtr ChangeLogCache;

    TResponseKeeperConfigPtr ResponseKeeper;

    TPersistentStateManagerConfig()
    {
        Register("changelogs", ChangeLogs);
        Register("snapshots", Snapshots);
        Register("max_changes_between_snapshots", MaxChangesBetweenSnapshots)
            .Default()
            .GreaterThan(0);
        Register("rpc_timeout", RpcTimeout)
            .Default(TDuration::MilliSeconds(3000));
        Register("cell", Cell)
            .DefaultNew();
        Register("election", Election)
            .DefaultNew();
        Register("changelog_downloader", ChangeLogDownloader)
            .DefaultNew();
        Register("snapshot_downloader", SnapshotDownloader)
            .DefaultNew();
        Register("follower_pinger", FollowerPinger)
            .DefaultNew();
        Register("quorum_tracker", QuorumTracker)
            .DefaultNew();
        Register("leader_committer", LeaderCommitter)
            .DefaultNew();
        Register("snapshot_builder", SnapshotBuilder)
            .DefaultNew();
        Register("change_log_cache", ChangeLogCache)
            .DefaultNew();
        Register("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Master discovery configuration.
struct TMasterDiscoveryConfig
    : public TYsonSerializable
{
    //! List of peer addresses.
    std::vector<Stroka> Addresses;

    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    //! Master connection priority. 
    int ConnectionPriority;

    TMasterDiscoveryConfig()
    {
        Register("addresses", Addresses)
            .NonEmpty();
        Register("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(5));
        Register("connection_priority", ConnectionPriority)
            .InRange(0, 6)
            .Default(6);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
