#pragma once

#include "common.h"

#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/election/leader_lookup.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Describes a chunk location.
struct TLocationConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TLocationConfig> TPtr;

    //! Location root path.
    Stroka Path;

    //! Maximum space chunks are allowed to occupy.
    //! (0 indicates to occupy all available space on drive).
    i64 Quota;

    //! Consider the location to be full when left space is less than #LowWatermark
    i64 LowWatermark;

    //! Aborts all uploading when left space is less than #HighWatermark
    i64 HighWatermark;

    TLocationConfig()
    {
        Register("path", Path).NonEmpty();
        Register("quota", Quota).Default(0);
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
            ythrow yexception() << "\"high_watermark\" cannot be more than \"low_watermark\"";
        }
    }
};

// TODO(roizner): It should be in block_table.h, but we cannot place it there because of cross-includes!
// TODO(roizner): Or merge it into TChunkHolderConfig
struct TBlockTableConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TBlockTableConfig> TPtr;

    int MaxPeersPerBlock;
    TDuration SweepPeriod;

    TBlockTableConfig()
    {
        Register("max_peers_per_block", MaxPeersPerBlock)
            .GreaterThan(0)
            .Default(64);
        Register("sweep_period", SweepPeriod)
            .Default(TDuration::Minutes(10));
    }
};

//! Describes a configuration of TChunkHolder.
struct TChunkHolderConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TChunkHolderConfig> TPtr;

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
    
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Timeout for RPC requests to masters.
    TDuration MasterRpcTimeout;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    //! Period between peer updates (see TPeerUpdater).
    TDuration PeerUpdatePeriod;

    //! Updated expiration timeout (see TPeerUpdater).
    TDuration PeerUpdateExpirationTimeout;

    //! Peer address to publish. Not registered.
    Stroka PeerAddress;

    i64 ResponseThrottlingSize;

    //! Regular storage locations.
    yvector<TLocationConfig::TPtr> ChunkStorageLocations;

    //! Location used for caching chunks.
    TLocationConfig::TPtr ChunkCacheLocation;

    //! Remote reader configuration used to download chunks into cache.
    NChunkClient::TRemoteReaderConfig::TPtr CacheRemoteReader;

    //! Sequential reader configuration used to download chunks into cache.
    NChunkClient::TSequentialReader::TConfig::TPtr CacheSequentialReader;

    TBlockTableConfig::TPtr BlockTable;

    //! Masters configuration.
    NElection::TLeaderLookup::TConfig::TPtr Masters;
    
    //! Constructs a default instance.
    /*!
     *  By default, no master connection is configured. The holder will operate in
     *  a standalone mode, which only makes sense for testing purposes.
     */
    TChunkHolderConfig()
    {
        // TODO: consider GreaterThan(0)

        Register("max_cached_blocks_size", MaxCachedBlocksSize).GreaterThan(0).Default(1024 * 1024);
        Register("max_cached_readers", MaxCachedReaders).GreaterThan(0).Default(10);
        Register("session_timeout", SessionTimeout).Default(TDuration::Seconds(15));
        Register("heartbeat_period", HeartbeatPeriod).Default(TDuration::Seconds(5));
        Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
        Register("rpc_port", RpcPort).Default(9000);
        Register("monitoring_port", MonitoringPort).Default(10000);
        Register("peer_update_period", PeerUpdatePeriod).Default(TDuration::Seconds(30));
        Register("peer_update_expiration_timeout", PeerUpdateExpirationTimeout).Default(TDuration::Seconds(40));
        Register("response_throttling_size", ResponseThrottlingSize).GreaterThan(0).Default(500 * 1024 * 1024);
        Register("chunk_store_locations", ChunkStorageLocations).NonEmpty();
        Register("chunk_cache_location", ChunkCacheLocation);
        Register("cache_remote_reader", CacheRemoteReader).DefaultNew();
        Register("cache_sequential_reader", CacheSequentialReader).DefaultNew();
        Register("block_table", BlockTable).DefaultNew();
        Register("masters", Masters);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
