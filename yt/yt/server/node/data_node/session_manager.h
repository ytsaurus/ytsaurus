#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk uploads.
/*!
 *  Thread affinity: any
 */
class TSessionManager
    : public TRefCounted
{
public:
    TSessionManager(
        TDataNodeConfigPtr config,
        IBootstrap* bootstrap);

    void Initialize();

    //! Starts a new chunk upload session.
    /*!
     *  Chunk files are opened asynchronously, however the call returns immediately.
     */
    ISessionPtr StartSession(TSessionId sessionId, const TSessionOptions& options);

    //! Finds session by session ID. Returns |nullptr| if no session is found.
    //! Session ID must not specify AllMediaIndex as medium index.
    ISessionPtr FindSession(TSessionId sessionId);

    //! Finds session by session ID. Throws if no session is found.
    //! Session ID must not specify AllMediaIndex as medium index.
    ISessionPtr GetSessionOrThrow(TSessionId sessionId);

    //! Returns the number of currently active sessions of a given type.
    int GetSessionCount(ESessionType type);

    //! Returns the flags indicating if new write sessions are disabled.
    bool GetDisableWriteSessions();

    //! Updates the flags indicating if new write sessions are disabled.
    void SetDisableWriteSessions(bool value);

private:
    const TDataNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SessionMapLock_);
    THashMap<TSessionId, ISessionPtr> SessionMap_;

    std::atomic<bool> DisableWriteSessions_ = false;

    ISessionPtr CreateSession(TSessionId sessionId, const TSessionOptions& options);

    void OnSessionLeaseExpired(TSessionId sessionId);
    void OnSessionFinished(const TWeakPtr<ISession>& weakSession, const TError& error);

    void RegisterSession(const ISessionPtr& session);
    void UnregisterSession(const ISessionPtr& session);

    void OnMasterDisconnected();

    void OnChunkRemovalScheduled(const IChunkPtr& chunk);
};

DEFINE_REFCOUNTED_TYPE(TSessionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

