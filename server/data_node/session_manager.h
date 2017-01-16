#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk uploads.
/*!
 *  Thread affinity: ControlThread
 */
class TSessionManager
    : public TRefCounted
{
public:
    TSessionManager(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Starts a new chunk upload session.
    /*!
     *  Chunk files are opened asynchronously, however the call returns immediately.
     */
    ISessionPtr StartSession(const TChunkId& chunkId, const TSessionOptions& options);

    //! Finds session by chunk id. Returns |nullptr| if no session is found.
    ISessionPtr FindSession(const TChunkId& chunkId);

    //! Finds session by chunk id. Throws if no session is found.
    ISessionPtr GetSession(const TChunkId& chunkId);

    //! Returns the number of currently active sessions of a given type.
    int GetSessionCount(ESessionType type);

    //! Returns the list of all registered sessions.
    std::vector<ISessionPtr> GetSessions();

private:
    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    yhash_map<TChunkId, ISessionPtr> SessionMap_;
    TEnumIndexedVector<NProfiling::TSimpleCounter, ESessionType> PerTypeSessionCounters_;


    ISessionPtr CreateSession(const TChunkId& chunkId, const TSessionOptions& options);

    void OnSessionLeaseExpired(const TChunkId& chunkId);
    void OnSessionFinished(ISession* session, const TError& error);

    void RegisterSession(ISessionPtr session);
    void UnregisterSession(ISessionPtr session);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TSessionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

