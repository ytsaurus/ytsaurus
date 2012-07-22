#pragma once

#include "public.h"
#include "chunk_holder_service_proxy.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/misc/lease_manager.h>
#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Represents a chunk upload in progress.
class TSession
    : public TRefCounted
{
public:
    TSession(
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
    NProto::TChunkInfo GetChunkInfo() const;

    //! Returns a block that is still in the session window.
    TSharedRef GetBlock(i32 blockIndex);

    //! Puts a block into the window.
    void PutBlock(
        i32 blockIndex,
        const TSharedRef& data,
        bool enableCaching);

    //! Flushes a block and moves the window
    /*!
     * The operation is asynchronous. It returns a result that gets set
     * when the actual flush happens. Once a block is flushed, the next block becomes
     * the first one in the window.
     */
    TFuture<void> FlushBlock(i32 blockIndex);

    //! Renews the lease.
    void RenewLease();

private:
    friend class TSessionManager;

    typedef TChunkHolderServiceProxy TProxy;
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
            , IsWritten(NewPromise<TVoid>())
        { }

        ESlotState State;
        TSharedRef Block;
        TPromise<TVoid> IsWritten;
    };

    typedef std::vector<TSlot> TWindow;

    TBootstrap* Bootstrap;
    TChunkId ChunkId;
    TLocationPtr Location;

    TWindow Window;
    i32 WindowStart;
    i64 Size;

    Stroka FileName;
    NChunkClient::TFileWriterPtr Writer;

    TLeaseManager::TLease Lease;

    IInvokerPtr WriteInvoker;

    NLog::TTaggedLogger Logger;

    TFuture<TChunkPtr> Finish(const NProto::TChunkMeta& chunkMeta);
    void Cancel(const TError& error);

    void SetLease(TLeaseManager::TLease lease);
    void CloseLease();

    bool IsInWindow(i32 blockIndex);
    void VerifyInWindow(i32 blockIndex);
    TSlot& GetSlot(i32 blockIndex);
    void ReleaseBlocks(i32 flushedBlockIndex);

    void OpenFile();
    void DoOpenFile();

    TFuture<TVoid> AbortWriter();
    TVoid DoAbortWriter();
    TVoid OnWriterAborted(TVoid);

    TFuture<TVoid> CloseFile(const NProto::TChunkMeta& chunkMeta);
    TVoid DoCloseFile(const NProto::TChunkMeta& chunkMeta);
    TChunkPtr OnFileClosed(TVoid);

    TVoid DoWrite(const TSharedRef& block, i32 blockIndex);
    void OnBlockWritten(i32 blockIndex, TVoid);

    void OnBlockFlushed(i32 blockIndex, TVoid);

    void ReleaseSpaceOccupiedByBlocks();
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
    TFuture<TChunkPtr> FinishSession(
        TSessionPtr session,
        const NProto::TChunkMeta& chunkMeta);

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
    TChunkPtr OnSessionFinished(TSessionPtr session, TChunkPtr chunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

