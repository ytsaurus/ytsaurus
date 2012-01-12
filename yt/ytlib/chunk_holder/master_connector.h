#pragma once

#include "common.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "session_manager.h"
#include "job_executor.h"
#include "chunk_service.pb.h"

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
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMasterConnector> TPtr;
    typedef TChunkHolderConfig TConfig;

    //! Creates an instance.
    TMasterConnector(
        TConfig* config,
        TChunkStore* chunkStore,
        TChunkCache* chunkCache,
        TSessionManager* sessionManager,
        TJobExecutor* jobExecutor,
        IInvoker* serviceInvoker);

private:
    typedef NChunkServer::TChunkServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef yhash_set<TChunk::TPtr> TChunks;

    //! Special id value indicating that the holder is not registered.
    static const int InvalidHolderId = -1;

    TConfig::TPtr Config;
    
    TChunkStore::TPtr ChunkStore;
    TChunkCache::TPtr ChunkCache;
    TSessionManager::TPtr SessionManager;
    TJobExecutor::TPtr JobExecutor;

    //! All state modifications are carried out via this invoker.
    IInvoker::TPtr ServiceInvoker;
    
    //! Indicates if the holder is currently registered at the master.
    bool Registered;
    
    //! Indicates if the holder must send the delta of its chunk set during the next heartbeat.
    /*! 
     *  When the holder is just registered, this flag is false.
     *  It becomes true upon a first successful heartbeat.
     *  The delta is stored in #AddedChunks and #RemovedChunks.
     */
    bool IncrementalHeartbeat;

    //! Current id assigned by the master, #InvalidHolderId if not registered.
    int HolderId;

    //! Proxy for the master.
    THolder<TProxy> Proxy;
    
    //! Local address of the holder.
    /*!
     *  This address is computed during initialization by combining the host name (returned by #HostName)
     *  and the port number in #Config->
     */
    Stroka Address;
    
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

    //! Sends out a heartbeat.
    void SendHeartbeat();

    //! Constructs a protobuf info for an added chunk.
    static NChunkServer::NProto::TReqHolderHeartbeat::TChunkAddInfo GetAddInfo(const TChunk* chunk);

    //! Constructs a protobuf info for a removed chunk.
    static NChunkServer::NProto::TReqHolderHeartbeat::TChunkRemoveInfo GetRemoveInfo(const TChunk* chunk);

    //! Handles heartbeat response.
    void OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response);

    //! Handles error during a registration or a heartbeat.
    void OnDisconnected();

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
