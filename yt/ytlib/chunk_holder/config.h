#pragma once

#include "public.h"
#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

struct TPeerBlockTableConfig
    : public TConfigurable
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
    : public TConfigurable
{
    //! Location root path.
    Stroka Path;

    //! Maximum space chunks are allowed to occupy.
    //! (If not initialized then indicates to occupy all available space on drive).
    TNullable<i64> Quota;

    //! Consider the location to be full when left space is less than #LowWatermark
    i64 LowWatermark;

    //! Aborts all uploading when left space is less than #HighWatermark
    i64 HighWatermark;

    TLocationConfig()
    {
        Register("path", Path).NonEmpty();
        Register("quota", Quota).Default(TNullable<i64>());
        Register("low_watermark", LowWatermark)
            .GreaterThan(0)
            .Default(1024 * 1024 * 1024); // 1 G
        Register("high_watermark", HighWatermark)
            .GreaterThan(0)
            .Default(100 * 1024 * 1024); // 100 Mb
    }

    virtual void DoValidate() const
    {
        if (HighWatermark > LowWatermark) {
            ythrow yexception() << "\"high_watermark\" cannot greater than \"low_watermark\"";
        }
    }
};

//! Describes a configuration of TChunkHolder.
struct TChunkHolderConfig
    : public TConfigurable
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
     * blocks uploaded or sent to other chunk holders). Otherwise
     * the session expires.
     */
    TDuration SessionTimeout;
    
    //! Timeout for "PutBlock" requests to other holders.
    TDuration HolderRpcTimeout;

    //! Period between peer updates (see TPeerBlockUpdater).
    TDuration PeerUpdatePeriod;

    //! Updated expiration timeout (see TPeerBlockUpdater).
    TDuration PeerUpdateExpirationTimeout;

    //! Data block responses are throttled when the out-queue reaches this size.
    i64 ResponseThrottlingSize;

    //! Regular storage locations.
    yvector<TLocationConfigPtr> StoreLocations;

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

    //! Constructs a default instance.
    /*!
     *  By default, no master connection is configured. The holder will operate in
     *  a standalone mode, which only makes sense for testing purposes.
     */
    TChunkHolderConfig()
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
            .Default(TDuration::Seconds(15));
        Register("holder_rpc_timeout", HolderRpcTimeout)
            .Default(TDuration::Seconds(15));
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
        Register("peer_block_table", PeerBlockTable)
            .DefaultNew();
        Register("replication_remote_writer", ReplicationRemoteWriter)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
