#pragma once

#include "public.h"

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

struct TPeerBlockTableConfig
    : public TYsonSerializable
{
    int MaxPeersPerBlock;
    TDuration SweepPeriod;

    TPeerBlockTableConfig()
    {
        Register("max_peers_per_block", MaxPeersPerBlock)
            .GreaterThan(0)
            .Default(64);
        Register("sweep_period", SweepPeriod)
            .Default(TDuration::Minutes(10));
    }
};

//! Describes a chunk location.
struct TLocationConfig
    : public TYsonSerializable
{
    //! Location root path.
    Stroka Path;

    //! Maximum space chunks are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    TNullable<i64> Quota;

    //! The location is considered to be full when left space is less than #LowWatermark.
    i64 LowWatermark;

    //! All uploads to the location are aborted when left space is less than #HighWatermark.
    i64 HighWatermark;

    TLocationConfig()
    {
        Register("path", Path).NonEmpty();
        Register("quota", Quota).Default(TNullable<i64>());
        Register("low_watermark", LowWatermark)
            .GreaterThanOrEqual(0)
            .Default((i64) 20 * 1024 * 1024 * 1024); // 20 Gb
        Register("high_watermark", HighWatermark)
            .GreaterThanOrEqual(0)
            .Default((i64) 10 * 1024 * 1024 * 1024); // 10 Gb
    }

    virtual void DoValidate() const
    {
        if (HighWatermark > LowWatermark) {
            THROW_ERROR_EXCEPTION("\"high_watermark\" cannot greater than \"low_watermark\"");
        }
    }
};

//! Describes a configuration of TDiskHealthChecker.
struct TDiskHealthCheckerConfig
    : public TYsonSerializable
{
    //! Period between consequent checks.
    TDuration CheckPeriod;

    //! Size of the test file.
    i64 TestSize;

    //! Maximum time allowed for execution of a single check.
    TDuration Timeout;

    TDiskHealthCheckerConfig()
    {
        Register("check_period", CheckPeriod)
            .Default(TDuration::Minutes(1));
        Register("test_size", TestSize)
            .InRange(0, (i64) 1024 * 1024 * 1024)
            .Default((i64) 1024 * 1024);
        Register("timeout", Timeout)
            .Default(TDuration::Seconds(5));
    }
};

//! Describes a configuration of TChunkHolder.
struct TDataNodeConfig
    : public TYsonSerializable
{
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat
    TDuration HeartbeatSplay;

    //! Timeout for FullHeartbeat requests.
    /*!
     *  This is usually much larger then the default RPC timeout.
     */
    TDuration FullHeartbeatTimeout;

    //! Block cache size (in bytes).
    i64 MaxCachedBlocksSize;

    //! Maximum number opened files in cache.
    int MaxCachedReaders;

    //! Upload session timeout.
    /*!
     * Some activity must be happening in a session regularly (i.e. new
     * blocks uploaded or sent to other data nodes). Otherwise
     * the session expires.
     */
    TDuration SessionTimeout;
    
    //! Timeout for "PutBlock" requests to other data nodes.
    TDuration NodeRpcTimeout;

    //! Period between peer updates (see TPeerBlockUpdater).
    TDuration PeerUpdatePeriod;

    //! Updated expiration timeout (see TPeerBlockUpdater).
    TDuration PeerUpdateExpirationTimeout;

    //! Data block responses are throttled when the out-queue reaches this size.
    i64 ResponseThrottlingSize;

    //! Regular storage locations.
    std::vector<TLocationConfigPtr> StoreLocations;

    //! Cached chunks location.
    TLocationConfigPtr CacheLocation;

    //! Remote reader configuration used to download chunks into cache.
    NChunkClient::TRemoteReaderConfigPtr CacheRemoteReader;

    //! Sequential reader configuration used to download chunks into cache.
    NChunkClient::TSequentialReaderConfigPtr CacheSequentialReader;

    //! Remote writer configuration used to replicate chunks.
    NChunkClient::TRemoteWriterConfigPtr ReplicationRemoteWriter;

    //! Keeps chunk peering information.
    TPeerBlockTableConfigPtr PeerBlockTable;

    //! Runs periodic checks against disks.
    TDiskHealthCheckerConfigPtr DiskHealthChecker;

    //! Constructs a default instance.
    /*!
     *  By default, no master connection is configured. The holder will operate in
     *  a standalone mode, which only makes sense for testing purposes.
     */
    TDataNodeConfig()
    {
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        Register("heartbeat_splay", HeartbeatSplay)
            .Default(TDuration::Seconds(5));
        Register("full_heartbeat_timeout", FullHeartbeatTimeout)
            .Default(TDuration::Seconds(60));
        Register("max_cached_blocks_size", MaxCachedBlocksSize)
            .GreaterThan(0)
            .Default(1024 * 1024);
        Register("max_cached_readers", MaxCachedReaders)
            .GreaterThan(0)
            .Default(10);
        Register("session_timeout", SessionTimeout)
            .Default(TDuration::Seconds(120));
        Register("node_rpc_timeout", NodeRpcTimeout)
            .Default(TDuration::Seconds(120));
        Register("peer_update_period", PeerUpdatePeriod)
            .Default(TDuration::Seconds(30));
        Register("peer_update_expiration_timeout", PeerUpdateExpirationTimeout)
            .Default(TDuration::Seconds(40));
        Register("response_throttling_size", ResponseThrottlingSize)
            .GreaterThan(0)
            .Default(500 * 1024 * 1024);
        Register("store_locations", StoreLocations)
            .NonEmpty();
        Register("cache_location", CacheLocation)
            .DefaultNew();
        Register("cache_remote_reader", CacheRemoteReader)
            .DefaultNew();
        Register("cache_sequential_reader", CacheSequentialReader)
            .DefaultNew();
        Register("replication_remote_writer", ReplicationRemoteWriter)
            .DefaultNew();
        Register("peer_block_table", PeerBlockTable)
            .DefaultNew();
        Register("disk_health_checker", DiskHealthChecker)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
