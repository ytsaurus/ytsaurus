#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>
#include <ytlib/election/leader_lookup.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectorConfig
    : public TConfigurable
{
    //! Peer address to publish. Not registered.
    Stroka PeerAddress;

    //! Masters configuration.
    NElection::TLeaderLookup::TConfig::TPtr Masters;

    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Timeout for FullHeartbeat requests.
    TDuration FullHeartbeatTimeout;

    TMasterConnectorConfig()
    {
        Register("masters", Masters);
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        Register("full_heartbeat_timeout", FullHeartbeatTimeout)
            .Default(TDuration::Seconds(60));
    }
};

//! Mediates connection between the holder and its master.
/*!
 *  This class is responsible for registering the holder and sending
 *  heartbeats. In particular, it reports chunk deltas to the master
 *  and manages jobs.
 */
class TMasterConnector
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TMasterConnector> TPtr;

    //! Creates an instance.
    TMasterConnector(TMasterConnectorConfig* config, TBootstrap* bootstrap);

    //! Starts interaction with master.
    void Start();

    //! Returns the channel for communicating with the leader.
    NRpc::IChannel::TPtr GetLeaderChannel() const;

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef yhash_set<TChunkPtr> TChunks;

    //! Special id value indicating that the holder is not registered.
    static const int InvalidHolderId = -1;

    //! Connector configuration.
    TMasterConnectorConfigPtr Config;

    //! The bootstrap that owns us.
    TBootstrap* Bootstrap;

    DECLARE_ENUM(EState,
        (NotRegistered)
        (Registered)
        (FullHeartbeatReported)
    );

    //! The current connection state.
    EState State;
    
    //! Current id assigned by the master, #InvalidHolderId if not registered.
    int HolderId;

    //! Channel to the leader.
    NRpc::IChannel::TPtr LeaderChannel;

    //! Proxy for the master.
    THolder<TProxy> Proxy;
    
    //! Chunks that were added since the last successful heartbeat.
    TChunks AddedSinceLastSuccess;

    //! Store chunks that were removed since the last successful heartbeat.
    TChunks RemovedSinceLastSuccess;

    //! Store chunks that were reported added at the last heartbeat (for which no reply is received yet).
    TChunks ReportedAdded;

    //! Store chunks that were reported removed at the last heartbeat (for which no reply is received yet).
    TChunks ReportedRemoved;

    //! Schedules a heartbeat via TDelayedInvoker.
    void ScheduleHeartbeat();

    //! Invoked when a heartbeat must be sent.
    void OnHeartbeat();

    //! Sends out a registration request.
    void SendRegister();

    //! Computes the current holder statistics.
    NChunkServer::NProto::THolderStatistics ComputeStatistics();

    //! Handles registration response.
    void OnRegisterResponse(TProxy::TRspRegisterHolder::TPtr response);

    //! Sends out a full heartbeat.
    void SendFullHeartbeat();

    //! Sends out an incremental heartbeat.
    void SendIncrementalHeartbeat();

    //! Constructs a protobuf info for an added chunk.
    static NChunkServer::NProto::TChunkAddInfo GetAddInfo(const TChunk* chunk);

    //! Constructs a protobuf info for a removed chunk.
    static NChunkServer::NProto::TChunkRemoveInfo GetRemoveInfo(const TChunk* chunk);

    //! Handles full heartbeat response.
    void OnFullHeartbeatResponse(TProxy::TRspFullHeartbeat::TPtr response);

    //! Handles incremental heartbeat response.
    void OnIncrementalHeartbeatResponse(TProxy::TRspIncrementalHeartbeat::TPtr response);

    //! Handles errors occurring during heartbeats.
    void OnHeartbeatError(const TError& error);

    //! Handles error during a registration or a heartbeat.
    void Disconnect();

    //! Handles registration of new chunks.
    /*!
     *  Places the chunk into a list and reports its arrival
     *  to the master upon a next heartbeat.
     */
    void OnChunkAdded(TChunk* chunk);

    //! Handles removal of existing chunks.
    /*!
     *  Places the chunk into a list and reports its removal
     *  to the master upon a next heartbeat.
     */
    void OnChunkRemoved(TChunk* chunk);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
