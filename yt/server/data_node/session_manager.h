#pragma once

#include "public.h"

#include <ytlib/misc/lease_manager.h>

#include <ytlib/concurrency/thread_affinity.h>

#include <ytlib/concurrency/throughput_throttler.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/profiling/profiler.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Represents a chunk upload in progress.
class TSession
    : public TRefCounted
{
public:
    TSession(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap,
        const TChunkId& chunkId,
        EWriteSessionType type,
        bool syncOnClose,
        TLocationPtr location);

    //! Starts the session.
    void Start();

    ~TSession();

    //! Returns the TChunkId being uploaded.
    TChunkId GetChunkId() const;

    //! Returns session type provided by the client during handshake.
    EWriteSessionType GetType() const;

    //! Returns target chunk location.
    TLocationPtr GetLocation() const;

    //! Returns the total data size received so far.
    i64 GetSize() const;

    //! Returns the number of blocks that have already been flushed out of the window.
    int GetWrittenBlockCount() const;

    //! Returns the info of the just-uploaded chunk
    NChunkClient::NProto::TChunkInfo GetChunkInfo() const;

    //! Puts a contiguous range of blocks into the window.
    TAsyncError PutBlocks(
        int startblockIndex,
        const std::vector<TSharedRef>& blocks,
        bool enableCaching);

    //! Sends a range of blocks (from the current window) to another data node.
    TAsyncError SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target);

    //! Flushes a block and moves the window
    /*!
     * The operation is asynchronous. It returns a result that gets set
     * when the actual flush happens. Once a block is flushed, the next block becomes
     * the first one in the window.
     */
    TAsyncError FlushBlock(int blockIndex);

    //! Renews the lease.
    void Ping();

private:
    friend class TSessionManager;

    typedef NChunkClient::TDataNodeServiceProxy TProxy;

    DECLARE_ENUM(ESlotState,
        (Empty)
        (Received)
        (Written)
    );

    struct TSlot
    {
        TSlot()
            : State(ESlotState::Empty)
            , IsWritten(NewPromise())
        { }

        ESlotState State;
        TSharedRef Block;
        TPromise<void> IsWritten;
    };

    typedef std::vector<TSlot> TWindow;

    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    TChunkId ChunkId;
    EWriteSessionType Type;
    bool SyncOnClose;
    TLocationPtr Location;

    TError Error;
    TWindow Window;
    int WindowStartIndex;
    int WriteIndex;
    i64 Size;

    Stroka FileName;
    NChunkClient::TFileWriterPtr Writer;

    TLeaseManager::TLease Lease;

    IInvokerPtr WriteInvoker;

    NLog::TTaggedLogger Logger;
    NProfiling::TProfiler Profiler;

    static TAsyncError DoSendBlocks(TProxy::TReqPutBlocksPtr req);

    TFuture< TErrorOr<TChunkPtr> > Finish(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    void Cancel(const TError& error);

    void SetLease(TLeaseManager::TLease lease);
    void CloseLease();

    bool IsInWindow(int blockIndex);
    void VerifyInWindow(int blockIndex);
    TSlot& GetSlot(int blockIndex);
    void ReleaseBlocks(int flushedBlockIndex);
    TSharedRef GetBlock(int blockIndex);
    void MarkAllSlotsWritten();

    void OpenFile();
    void DoOpenFile();

    TAsyncError AbortWriter();
    TError DoAbortWriter();
    TError OnWriterAborted(TError error);

    TAsyncError CloseFile(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    TError DoCloseFile(const NChunkClient::NProto::TChunkMeta& chunkMeta);
    TErrorOr<TChunkPtr> OnFileClosed(TError error);

    void EnqueueWrites();
    TError DoWriteBlock(const TSharedRef& block, int blockIndex);
    void OnBlockWritten(int blockIndex, TError error);

    TError OnBlockFlushed(int blockIndex);

    void ReleaseSpaceOccupiedByBlocks();

    void OnIOError(const TError& error);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

};

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk uploads.
/*!
 *  Thread affinity: Control
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
     *  Chunk file is opened asynchronously, however the call returns immediately.
     */
    TSessionPtr StartSession(
        const TChunkId& chunkId,
        EWriteSessionType type,
        bool syncOnClose);

    //! Completes an earlier opened upload session.
    /*!
     *  The call returns a result that gets set when the session is finished.
     */
    TFuture< TErrorOr<TChunkPtr> > FinishSession(
        TSessionPtr session,
        const NChunkClient::NProto::TChunkMeta& chunkMeta);

    //! Cancels an earlier opened upload session.
    /*!
     *  Chunk file is closed asynchronously, however the call returns immediately.
     */
    void CancelSession(TSessionPtr session, const TError& error);

    //! Finds a session by TChunkId. Returns NULL when no session is found.
    TSessionPtr FindSession(const TChunkId& chunkId) const;

    //! Returns the number of currently active sessions of a given type.
    int GetSessionCount(EWriteSessionType type) const;

    //! Returns the list of all registered sessions.
    std::vector<TSessionPtr> GetSessions() const;

    //! Returns the number of bytes pending for write.
    /*!
     *  Thread affinity: any
     */
    i64 GetPendingWriteSize() const;

private:
    friend class TSession;

    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    typedef yhash_map<TChunkId, TSessionPtr> TSessionMap;
    TSessionMap SessionMap;
    std::vector<int> SessionCounts;
    TAtomic PendingWriteSize;

    void OnLeaseExpired(TSessionPtr session);

    TErrorOr<TChunkPtr> OnSessionFinished(
        TSessionPtr session,
        TErrorOr<TChunkPtr> chunkOrError);

    void RegisterSession(TSessionPtr session);
    void UnregisterSession(TSessionPtr session);

    void UpdatePendingWriteSize(i64 delta);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

