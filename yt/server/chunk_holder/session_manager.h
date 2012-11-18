#pragma once

#include "public.h"

#include <ytlib/misc/lease_manager.h>
#include <ytlib/misc/thread_affinity.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Represents a chunk upload in progress.
class TSession
    : public TRefCounted
{
public:
    TSession(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap,
        const TChunkId& chunkId,
        TLocationPtr location);

    //! Starts the session.
    void Start();

    ~TSession();

    //! Returns the TChunkId being uploaded.
    TChunkId GetChunkId() const;

    //! Returns target chunk location.
    TLocationPtr GetLocation() const;

    //! Returns the total data size received so far.
    i64 GetSize() const;

    int GetWrittenBlockCount() const;

    //! Returns the info of the just-uploaded chunk
    NChunkClient::NProto::TChunkInfo GetChunkInfo() const;

    //! Puts a block into the window.
    void PutBlock(
        int blockIndex,
        const TSharedRef& data,
        bool enableCaching);

    //! Sends a range of blocks (from the current window) to another data node.
    TAsyncError SendBlocks(
        int startBlockIndex,
        int blockCount,
        const Stroka& targetAddress);

    //! Flushes a block and moves the window
    /*!
     * The operation is asynchronous. It returns a result that gets set
     * when the actual flush happens. Once a block is flushed, the next block becomes
     * the first one in the window.
     */
    TAsyncError FlushBlock(int blockIndex);

    //! Renews the lease.
    void RenewLease();

private:
    friend class TSessionManager;

    typedef NChunkClient::TDataNodeServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    DECLARE_ENUM(ESlotState,
        (Empty)
        (Received)
        (Written)
    );

    struct TSlot
    {
        TSlot()
            : State(ESlotState::Empty)
            , IsWritten(NewPromise<void>())
        { }

        ESlotState State;
        TSharedRef Block;
        TPromise<void> IsWritten;
    };

    typedef std::vector<TSlot> TWindow;

    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;
    TChunkId ChunkId;
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

    TFuture< TValueOrError<TChunkPtr> > Finish(const NChunkClient::NProto::TChunkMeta& chunkMeta);
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
    TValueOrError<TChunkPtr> OnFileClosed(TError error);

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
class TSessionManager
    : public TRefCounted
{
public:
    typedef std::vector<TSessionPtr> TSessions;

    //! Constructs a manager.
    TSessionManager(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap);

    //! Starts a new chunk upload session.
    TSessionPtr StartSession(const TChunkId& chunkId);

    //! Completes an earlier opened upload session.
    /*!
     * The call returns a result that gets set when the session is finished.
     */
    TFuture< TValueOrError<TChunkPtr> > FinishSession(
        TSessionPtr session,
        const NChunkClient::NProto::TChunkMeta& chunkMeta);

    //! Cancels an earlier opened upload session.
    /*!
     * Chunk file is closed asynchronously, however the call returns immediately.
     */
    void CancelSession(TSessionPtr session, const TError& error);

    //! Finds a session by TChunkId. Returns NULL when no session is found.
    TSessionPtr FindSession(const TChunkId& chunkId) const;

    //! Returns the number of currently active session.
    /*!
     * Thread safe.
     */
    int GetSessionCount() const;

    //! Returns the list of all registered sessions.
    TSessions GetSessions() const;

private:
    friend class TSession;

    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;

    typedef yhash_map<TChunkId, TSessionPtr> TSessionMap;
    TSessionMap SessionMap;
    TAtomic SessionCount;

    void OnLeaseExpired(TSessionPtr session);
    TValueOrError<TChunkPtr> OnSessionFinished(TSessionPtr session, TValueOrError<TChunkPtr> chunkOrError);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

