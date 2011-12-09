#pragma once

#include "../misc/common.h"
#include "../misc/config.h"

#include "chunk_holder_service_rpc.pb.h"
#include "chunk_service_rpc.pb.h"

#include "../chunk_client/common.h"
#include "../election/leader_lookup.h"
#include "../misc/guid.h"
#include "../logging/log.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Describes a chunk location.
struct TLocationConfig
    : TConfigBase
{
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
    : public TConfigBase

{
    //! Maximum number blocks in cache.
    int MaxCachedBlocks;

    //! Maximum number opened files in cache.
    int MaxCachedFiles;

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

    //// TODO: consider making per/location limit

    // TODO: killme
    Stroka NewConfigFileName;

    //! Describes regular storage locations.
    yvector<TLocationConfig> Locations;

    //! Masters configuration.
    /*!
     *  If no master addresses are given, the holder will operate in a standalone mode.
     */
    NElection::TLeaderLookup::TConfig Masters;
    
    //! Constructs a default instance.
    /*!
     *  By default, no master connection is configured. The holder will operate in
     *  a standalone mode, which only makes sense for testing purposes.
     */
    TChunkHolderConfig()
    {
        Register("max_cached_blocks", MaxCachedBlocks).GreaterThan(0).Default(10);
        Register("max_cached_files", MaxCachedFiles).GreaterThan(0).Default(10);
        Register("session_timeout", SessionTimeout).Default(TDuration::Seconds(15));
        Register("heartbeat_period", HeartbeatPeriod).Default(TDuration::Seconds(5));
        Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
        Register("rpc_port", RpcPort).Default(9000);
        Register("monitoring_port", MonitoringPort).Default(10001);
        //Register("locations", Locations);
        Register("masters", Masters);

        SetDefaults();
    }

    //! Reads configuration from JSON.
    void Read(TJsonObject* json);
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
