#pragma once

#include "common.h"

#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/remote_writer.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_holder/peer_block_table.h>
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

    //! Timeout for "PutBlock" requests to other holders.
    TDuration HolderRpcTimeout;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    //! Period between peer updates (see TPeerBlockUpdater).
    TDuration PeerUpdatePeriod;

    //! Updated expiration timeout (see TPeerBlockUpdater).
    TDuration PeerUpdateExpirationTimeout;

    //! Peer address to publish. Not registered.
    Stroka PeerAddress;

    i64 ResponseThrottlingSize;

    //! Regular storage locations.
    yvector<TLocationConfig::TPtr> ChunkStorageLocations;

    //! Cached chunks location.
    TLocationConfig::TPtr ChunkCacheLocation;

    //! Remote reader configuration used to download chunks into cache.
    NChunkClient::TRemoteReaderConfig::TPtr CacheRemoteReader;

    //! Sequential reader configuration used to download chunks into cache.
    NChunkClient::TSequentialReader::TConfig::TPtr CacheSequentialReader;

    //! Remote writer configuration used to replicate chunks.
    NChunkClient::TRemoteWriter::TConfig::TPtr ReplicationRemoteWriter;

    //! Keeps chunk peering information.
    TPeerBlockTable::TConfig::TPtr PeerBlockTable;

    //! Masters configuration.
    NElection::TLeaderLookup::TConfig::TPtr Masters;
    

    //! Constructs a default instance.
    /*!
     *  By default, no master connection is configured. The holder will operate in
     *  a standalone mode, which only makes sense for testing purposes.
     */
    TChunkHolderConfig()
    {
        Register("max_cached_blocks_size", MaxCachedBlocksSize)
            .GreaterThan(0)
            .Default(1024 * 1024);
        Register("max_cached_readers", MaxCachedReaders)
            .GreaterThan(0)
            .Default(10);
        Register("session_timeout", SessionTimeout)
            .Default(TDuration::Seconds(15));
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        Register("holder_rpc_timeout", HolderRpcTimeout)
            .Default(TDuration::Seconds(15));
        Register("rpc_port", RpcPort)
            .Default(9000);
        Register("monitoring_port", MonitoringPort)
            .Default(10000);
        Register("peer_update_period", PeerUpdatePeriod)
            .Default(TDuration::Seconds(30));
        Register("peer_update_expiration_timeout", PeerUpdateExpirationTimeout)
            .Default(TDuration::Seconds(40));
        Register("response_throttling_size", ResponseThrottlingSize)
            .GreaterThan(0)
            .Default(500 * 1024 * 1024);
        Register("chunk_store_locations", ChunkStorageLocations)
            .NonEmpty();
        Register("chunk_cache_location", ChunkCacheLocation);
        Register("cache_remote_reader", CacheRemoteReader)
            .DefaultNew();
        Register("cache_sequential_reader", CacheSequentialReader)
            .DefaultNew();
        Register("peer_block_table", PeerBlockTable)
            .DefaultNew();
        Register("masters", Masters);
        Register("replication_remote_writer", ReplicationRemoteWriter)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
