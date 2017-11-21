#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/session_id.h>

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
    using TSessionPtrList = SmallVector<ISessionPtr, 1>;

    TSessionManager(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Starts a new chunk upload session.
    /*!
     *  Chunk files are opened asynchronously, however the call returns immediately.
     */
    ISessionPtr StartSession(const TSessionId& sessionId, const TSessionOptions& options);

    //! Finds session by session ID. Returns |nullptr| if no session is found.
    //! Session ID must not specify AllMediaIndex as medium index.
    ISessionPtr FindSession(const TSessionId& sessionId);

    //! Finds session by session ID. Throws if no session is found.
    //! Session ID must not specify AllMediaIndex as medium index.
    ISessionPtr GetSession(const TSessionId& sessionId);

    //! Finds session by session ID. Returns empty vector if no sessions were found.
    //! If session ID specifies AllMediaIndex as medium index, all sessions
    //! matching chunk ID are returned.
    TSessionPtrList FindSessions(const TSessionId& sessionId);

    //! Finds session by session ID. Throws if no sessions were found (never
    //! returns empty vector).
    //! If session ID specifies AllMediaIndex as medium index, all sessions
    //! matching chunk ID are returned.
    TSessionPtrList GetSessions(const TSessionId& sessionId);

    //! Returns the number of currently active sessions of a given type.
    int GetSessionCount(ESessionType type);

    //! Returns the list of all registered sessions.
    std::vector<ISessionPtr> GetSessions();

private:
    const TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    THashMap<TSessionId, ISessionPtr> SessionMap_;
    TEnumIndexedVector<NProfiling::TSimpleCounter, ESessionType> PerTypeSessionCounters_;

    ISessionPtr CreateSession(const TSessionId& sessionId, const TSessionOptions& options);

    void OnSessionLeaseExpired(const TSessionId& sessionId);
    void OnSessionFinished(ISession* session, const TError& error);

    void RegisterSession(ISessionPtr session);
    void UnregisterSession(ISessionPtr session);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TSessionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

