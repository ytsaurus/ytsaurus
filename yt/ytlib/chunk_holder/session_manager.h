#pragma once

#include "public.h"
#include "chunk_holder_service_proxy.h"

#include <ytlib/chunk_client/file_writer.h>
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
        TSessionManagerPtr sessionManager,
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

    //! Returns the info of the just-uploaded chunk
    NProto::TChunkInfo GetChunkInfo() const;

    //! Returns a cached block that is still in the session window.
    TCachedBlockPtr GetBlock(i32 blockIndex);

    //! Puts a block into the window.
    void PutBlock(i32 blockIndex, const TSharedRef& data);

    //! Flushes a block and moves the window
    /*!
     * The operation is asynchronous. It returns a result that gets set
     * when the actual flush happens. Once a block is flushed, the next block becomes
     * the first one in the window.
     */
    TFuture<TVoid>::TPtr FlushBlock(i32 blockIndex);

    //! Renews the lease.
    void RenewLease();

private:
    friend class TSessionManager;

    typedef TChunkHolderServiceProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef NChunkHolder::NProto::TChunkAttributes TChunkAttributes;

    DECLARE_ENUM(ESlotState,
        (Empty)
        (Received)
        (Written)
    );

    struct TSlot
    {
        TSlot()
            : State(ESlotState::Empty)
            , Block(NULL)
            , IsWritten(New< TFuture<TVoid> >())
        { }

        ESlotState State;
        TCachedBlockPtr Block;
        TFuture<TVoid>::TPtr IsWritten;
    };

    typedef yvector<TSlot> TWindow;

    TSessionManagerPtr SessionManager;
    TChunkId ChunkId;
    TLocationPtr Location;
    
    TWindow Window;
    i32 WindowStart;
    i32 FirstUnwritten;
    i64 Size;

    Stroka FileName;
    NChunkClient::TChunkFileWriter::TPtr Writer;

    TLeaseManager::TLease Lease;

    NLog::TTaggedLogger Logger;

    TFuture<TChunkPtr>::TPtr Finish(const TChunkAttributes& attributes);
    void Cancel(const TError& error);

    void SetLease(TLeaseManager::TLease lease);
    void CloseLease();

    bool IsInWindow(i32 blockIndex);
    void VerifyInWindow(i32 blockIndex);
    TSlot& GetSlot(i32 blockIndex);
    void ReleaseBlocks(i32 flushedBlockIndex);

    IInvoker::TPtr GetIOInvoker();

    void OpenFile();
    void DoOpenFile();

    TFuture<TVoid>::TPtr DeleteFile(const TError& error);
    TVoid DoDeleteFile(const TError& error);
    TVoid OnFileDeleted(TVoid);

    TFuture<TVoid>::TPtr CloseFile(const TChunkAttributes& attributes);
    TVoid DoCloseFile(const TChunkAttributes& attributes);
    TChunkPtr OnFileClosed(TVoid);

    void EnqueueWrites();
    TVoid DoWrite(TCachedBlockPtr block, i32 blockIndex);
    void OnBlockWritten(i32 blockIndex, TVoid);

    TVoid OnBlockFlushed(i32 blockIndex, TVoid);

    void ReleaseSpaceOccupiedByBlocks();
};

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk uploads.
class TSessionManager
    : public TRefCounted
{
public:
    typedef yvector<TSessionPtr> TSessions;

    //! Constructs a manager.
    TSessionManager(
        TChunkHolderConfigPtr config,
        TBlockStorePtr blockStore,
        TChunkStorePtr chunkStore,
        IInvoker::TPtr serviceInvoker);

    //! Starts a new chunk upload session.
    TSessionPtr StartSession(const TChunkId& chunkId);

    //! Completes an earlier opened upload session.
    /*!
     * The call returns a result that gets set when the session is finished.
     */
    TFuture<TChunkPtr>::TPtr FinishSession(
        TSessionPtr session,
        const NChunkHolder::NProto::TChunkAttributes& attributes);

    //! Cancels an earlier opened upload session.
    /*!
     * Chunk file is closed asynchronously, however the call returns immediately.
     */
    void CancelSession(TSessionPtr session, const TError& error);

    //! Finds a session by TChunkId. Returns NULL when no session is found.
    TSessionPtr FindSession(const TChunkId& chunkId) const;

    //! Returns the number of currently active session.
    int GetSessionCount() const;

    //! Returns the list of all registered sessions.
    TSessions GetSessions() const;

private:
    friend class TSession;

    TChunkHolderConfigPtr Config;
    TBlockStorePtr BlockStore;
    TChunkStorePtr ChunkStore;
    IInvoker::TPtr ServiceInvoker;

    typedef yhash_map<TChunkId, TSessionPtr> TSessionMap;
    TSessionMap SessionMap;

    void OnLeaseExpired(TSessionPtr session);
    TChunkPtr OnSessionFinished(TSessionPtr session, TChunkPtr chunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

