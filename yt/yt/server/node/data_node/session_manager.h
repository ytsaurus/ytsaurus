#pragma once

#include "public.h"

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

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

    //! Finds session by chunk id. Returns |nullptr| if no session is found.
    ISessionPtr FindSession(TChunkId chunkId);

    //! Finds session by chunk id. Throws if no session is found.
    ISessionPtr GetSessionOrThrow(TChunkId chunkId);

    //! Returns the number of currently active sessions of a given type.
    int GetSessionCount(ESessionType type);

    //! Returns the flags indicating if new write sessions are disabled.
    bool GetDisableWriteSessions();

    //! Updates the flags indicating if new write sessions are disabled.
    void SetDisableWriteSessions(bool value);

    //! Cancel all location sessions.
    void CancelLocationSessions(const TChunkLocationPtr& location);

    bool CanPassSessionOutOfTurn(TChunkId chunkId);

    NYTree::IYPathServicePtr GetOrchidService();

private:
    struct TSessionCreatedAtSortIndex
    {
        TChunkId ChunkId;
        TInstant StartedAt;

        std::strong_ordering operator<=>(const TSessionCreatedAtSortIndex& other) const = default;
    };

    const TDataNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;
    const NYTree::IYPathServicePtr OrchidService_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SessionMapLock_);
    THashMap<TChunkId, ISessionPtr> SessionMap_;

    std::atomic<bool> DisableWriteSessions_ = false;

    std::set<TSessionCreatedAtSortIndex> SessionToCreatedAt_;

    ISessionPtr CreateSession(TSessionId sessionId, const TSessionOptions& options);

    void OnSessionLeaseExpired(TChunkId chunkId);
    void OnSessionFinished(const TWeakPtr<ISession>& weakSession, const TError& error);

    void RegisterSession(const ISessionPtr& session);
    void UnregisterSession(const ISessionPtr& session);

    void OnMasterDisconnected();

    void OnChunkRemovalScheduled(const IChunkPtr& chunk);

    void BuildOrchid(NYson::IYsonConsumer* consumer);

    NYTree::IYPathServicePtr CreateOrchidService();
};

DEFINE_REFCOUNTED_TYPE(TSessionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

