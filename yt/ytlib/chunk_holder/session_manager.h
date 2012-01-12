#pragma once

#include "block_store.h"
#include "chunk_store.h"
#include "chunk_holder_service_proxy.h"

#include "../chunk_client/file_writer.h"
#include "../misc/lease_manager.h"
#include "../logging/tagged_logger.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TSessionManager;

//! Represents a chunk upload in progress.
class TSession
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSession> TPtr;

    TSession(
        TSessionManager* sessionManager,
        const TChunkId& chunkId,
        TLocation* location);

    ~TSession();

    //! Returns the TChunkId being uploaded.
    TChunkId GetChunkId() const;

    //! Returns target chunk location.
    TLocation::TPtr GetLocation() const;

    //! Returns the total data size received so far.
    i64 GetSize() const;

    //! Returns the info of the just-uploaded chunk
    NProto::TChunkInfo GetChunkInfo() const;

    //! Returns a cached block that is still in the session window.
    TCachedBlock::TPtr GetBlock(i32 blockIndex);

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
        TCachedBlock::TPtr Block;
        TFuture<TVoid>::TPtr IsWritten;
    };

    typedef yvector<TSlot> TWindow;

    TIntrusivePtr<TSessionManager> SessionManager;
    TChunkId ChunkId;
    TLocation::TPtr Location;
    
    TWindow Window;
    i32 WindowStart;
    i32 FirstUnwritten;
    i64 Size;
    NProto::TChunkInfo ChunkInfo;
    bool HasChunkInfo;

    Stroka FileName;
    NChunkClient::TChunkFileWriter::TPtr Writer;

    TLeaseManager::TLease Lease;

    NLog::TTaggedLogger Logger;

    TFuture<TVoid>::TPtr Finish(const TChunkAttributes& attributes);
    void Cancel(const TError& error);

    void SetLease(TLeaseManager::TLease lease);
    void CloseLease();

    bool IsInWindow(i32 blockIndex);
    void VerifyInWindow(i32 blockIndex);
    TSlot& GetSlot(i32 blockIndex);
    void ReleaseBlocks(i32 flushedBlockIndex);

    IInvoker::TPtr GetInvoker();

    void OpenFile();
    void DoOpenFile();

    void DeleteFile(const TError& error);
    void DoDeleteFile(const TError& error);

    TFuture<TVoid>::TPtr CloseFile(const TChunkAttributes& attributes);
    TVoid DoCloseFile(const TChunkAttributes& attributes);

    void EnqueueWrites();
    TVoid DoWrite(TCachedBlock::TPtr block, i32 blockIndex);
    void OnBlockWritten(TVoid, i32 blockIndex);

    TVoid OnBlockFlushed(TVoid, i32 blockIndex);
};

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk uploads.
class TSessionManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSessionManager> TPtr;
    typedef yvector<TSession::TPtr> TSessions;

    //! Constructs a manager.
    TSessionManager(
        TChunkHolderConfig* config,
        TBlockStore* blockStore,
        TChunkStore* chunkStore,
        IInvoker* serviceInvoker);

    //! Starts a new chunk upload session.
    TSession::TPtr StartSession(
        const TChunkId& chunkId);

    //! Completes an earlier opened upload session.
    /*!
     * The call returns a result that gets set when the session is finished.
     */
    TFuture<TVoid>::TPtr FinishSession(
        TSession::TPtr session,
        const NChunkHolder::NProto::TChunkAttributes& attributes);

    //! Cancels an earlier opened upload session.
    /*!
     * Chunk file is closed asynchronously, however the call returns immediately.
     */
    void CancelSession(TSession* session, const TError& error);

    //! Finds a session by TChunkId. Returns NULL when no session is found.
    TSession::TPtr FindSession(const TChunkId& chunkId) const;

    //! Returns the number of currently active session.
    int GetSessionCount() const;

    //! Returns the list of all registered sessions.
    TSessions GetSessions() const;

private:
    friend class TSession;

    TChunkHolderConfig::TPtr Config;
    TBlockStore::TPtr BlockStore;
    TChunkStore::TPtr ChunkStore;
    IInvoker::TPtr ServiceInvoker;

    typedef yhash_map<TChunkId, TSession::TPtr> TSessionMap;
    TSessionMap SessionMap;

    void OnLeaseExpired(TSession::TPtr session);
    TVoid OnSessionFinished(TVoid, TSession::TPtr session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

