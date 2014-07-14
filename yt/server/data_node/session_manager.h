#pragma once

#include "public.h"

#include <core/concurrency/thread_affinity.h>

#include <server/cell_node/public.h>

#include <atomic>

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
    int GetSessionCount(EWriteSessionType type);

    //! Returns the list of all registered sessions.
    std::vector<ISessionPtr> GetSessions();

    //! Updates (increments or decrements) pending write size.
    /*!
     *  Thread affinity: any
     */
    void UpdatePendingWriteSize(i64 delta);

    //! Returns the number of bytes pending for write.
    /*!
     *  Thread affinity: any
     */
    i64 GetPendingWriteSize();

private:
    TDataNodeConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    yhash_map<TChunkId, ISessionPtr> SessionMap_;
    std::vector<int> PerTypeSessionCount_;
    std::atomic<i64> PendingWriteSize_;


    ISessionPtr CreateSession(const TChunkId& chunkId, const TSessionOptions& options);

    void OnSessionLeaseExpired(ISessionPtr session);
    void OnSessionFinished(ISession* session, const TError& error);

    void RegisterSession(ISessionPtr session);
    void UnregisterSession(ISessionPtr session);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TSessionManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

