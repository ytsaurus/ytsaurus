#pragma once

#include "../misc/common.h"
#include "../misc/configurable.h"

#include "chunk_holder_service.pb.h"
#include "chunk_service.pb.h"

#include "../chunk_client/common.h"
#include "../chunk_client/remote_reader.h"
#include "../chunk_client/sequential_reader.h"
#include "../election/leader_lookup.h"
#include "../misc/guid.h"
#include "../logging/log.h"

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

    //! Maximum space chunks are allowed to occupy (0 indicates no limit).
    i64 Quota;

    TLocationConfig()
    {
        Register("path", Path).NonEmpty();
        Register("quota", Quota).Default(0);
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

    i64 ResponseThrottlingSize;

    //! Regular storage locations.
    yvector<TLocationConfig::TPtr> ChunkStorageLocations;

    //! Location used for caching chunks.
    TLocationConfig::TPtr ChunkCacheLocation;

    //! Remote reader configuration used to download chunks into cache.
    NChunkClient::TRemoteReader::TConfig::TPtr CacheRemoteReader;

    //! Sequential reader configuration used to download chunks into cache.
    NChunkClient::TSequentialReader::TConfig::TPtr CacheSequentialReader;

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
        Register("response_throttling_size", ResponseThrottlingSize).GreaterThan(0).Default(500 * 1024 * 1024);
        Register("chunk_store_locations", ChunkStorageLocations).NonEmpty();
        Register("chunk_cache_location", ChunkCacheLocation);
        Register("cache_remote_reader", CacheRemoteReader);
        Register("cache_sequential_reader", CacheSequentialReader);
        Register("masters", Masters);
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: replace by NProto:THolderStatistics
struct THolderStatistics
{
    THolderStatistics()
        : AvailableSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
    { }

    i64 AvailableSpace;
    i64 UsedSpace;
    i32 ChunkCount;
    i32 SessionCount;

    static THolderStatistics FromProto(const NChunkServer::NProto::THolderStatistics& proto)
    {
        THolderStatistics result;
        result.AvailableSpace = proto.availablespace();
        result.UsedSpace = proto.usedspace();
        result.ChunkCount = proto.chunkcount();
        result.SessionCount = proto.sessioncount();
        return result;
    }

    NChunkServer::NProto::THolderStatistics ToProto() const
    {
        NChunkServer::NProto::THolderStatistics result;
        result.set_availablespace(AvailableSpace);
        result.set_usedspace(UsedSpace);
        result.set_chunkcount(ChunkCount);
        result.set_sessioncount(SessionCount);
        return result;
    }

    Stroka ToString() const
    {
        return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64 ", ChunkCount: %d, SessionCount: %d",
            AvailableSpace,
            UsedSpace,
            ChunkCount,
            SessionCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

DECLARE_PODTYPE(NYT::NChunkHolder::THolderStatistics);
