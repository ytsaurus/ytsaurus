#pragma once

#include "public.h"
//#include "bootstrap.h"
//#include "chunk.h"
//#include "chunk_service.pb.h"

#include <ytlib/rpc/channel.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

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
    TMasterConnector(TChunkHolderConfig* config, TBootstrap* bootstrap);

    //! Starts interaction with master.
    void Start();

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef yhash_set<TChunkPtr> TChunks;

    //! Special id value indicating that the holder is not registered.
    static const int InvalidHolderId = -1;

    //! Connector configuration.
    TChunkHolderConfigPtr Config;

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
