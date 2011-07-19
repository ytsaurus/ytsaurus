#pragma once

#include "common.h"
#include "block_store.h"
#include "chunk_store.h"
#include "chunk_holder_rpc.h"

#include "../misc/lease_manager.h"

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

    //! Constructs a new session.
    TSession(
        TIntrusivePtr<TSessionManager> sessionManager,
        const TChunkId& chunkId,
        int location,
        int windowSize);

    //! Returns TChunkId being uploaded.
    TChunkId GetChunkId() const;

    //! Returns target chunk location.
    int GetLocation() const;

    //! Returns the size of blocks received so far.
    i64 GetSize() const;

    //! Returns a cached block that is still in the session window.
    TCachedBlock::TPtr GetBlock(int blockIndex);

    //! Puts a block into the window.
    void PutBlock(
        i32 blockIndex,
        const TBlockId& blockId,
        const TSharedRef& data);

    //! Flushes a block and moves the window
    /*!
     * The operation is asynchronous. It returns a result that gets set
     * when the actual flush happens. Once a block is flushed, the next block becomes
     * the first one in the window.
     */
    TAsyncResult<TVoid>::TPtr FlushBlock(int blockIndex);

private:
    friend class TSessionManager;

    typedef TChunkHolderProxy::EErrorCode TErrorCode;
    typedef NRpc::TTypedServiceException<TErrorCode> TServiceException;

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
            , IsWritten(new TAsyncResult<TVoid>())
        { }

        ESlotState State;
        TCachedBlock::TPtr Block;
        TAsyncResult<TVoid>::TPtr IsWritten;
    };

    typedef yvector<TSlot> TWindow;

    TIntrusivePtr<TSessionManager> SessionManager;
    TChunkId ChunkId;
    int Location;
    
    TWindow Window;
    int WindowStart;
    int FirstUnwritten;
    i64 Size;

    Stroka FileName;
    THolder<TFile> File;

    TLeaseManager::TLease Lease;

    TAsyncResult<TVoid>::TPtr Finish();
    void Cancel();

    void SetLease(TLeaseManager::TLease lease);
    void RenewLease();
    void CloseLease();

    bool IsInWindow(int blockIndex);
    void VerifyInWindow(int blockIndex);
    TSlot& GetSlot(int index);
    void RotateWindow(int flushedBlockIndex);

    IInvoker::TPtr GetInvoker();

    void OpenFile();
    void DoOpenFile();

    void DeleteFile();
    void DoDeleteFile();

    TAsyncResult<TVoid>::TPtr CloseFile();
    TVoid DoCloseFile();

    void EnqueueWrites();
    TVoid DoWrite(TCachedBlock::TPtr block, int blockIndex);
    void OnBlockWritten(TVoid, int blockIndex);

    TVoid OnBlockFlushed(TVoid, int blockIndex);

};

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk uploads.
class TSessionManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSessionManager> TPtr;

    //! Constructs a manager.
    TSessionManager(
        const TChunkHolderConfig& config,
        TBlockStore::TPtr blockStore,
        TChunkStore::TPtr chunkStore,
        IInvoker::TPtr serviceInvoker);

    //! Starts a new chunk upload session.
    TSession::TPtr StartSession(
        const TChunkId& chunkId,
        int windowSize);

    //! Completes an earlier opened upload session.
    /*!
     * The call returns a result that gets set when the session is finished.
     */
    TAsyncResult<TVoid>::TPtr FinishSession(TSession::TPtr session);

    //! Cancels an earlier opened upload session.
    /*!
     * Chunk file is closed asynchronously, however the call returns immediately.
     */
    void CancelSession(TSession::TPtr session);

    //! Finds a session by TChunkId. Returns NULL when no session is found.
    TSession::TPtr FindSession(const TChunkId& chunkId);

private:
    friend class TSession;

    TChunkHolderConfig Config; // TODO: avoid copying
    TBlockStore::TPtr BlockStore;
    TChunkStore::TPtr ChunkStore;
    IInvoker::TPtr ServiceInvoker;

    TLeaseManager::TPtr LeaseManager;

    typedef yhash_map<TChunkId, TSession::TPtr, TGuidHash> TSessionMap;
    TSessionMap SessionMap;

    void OnLeaseExpired(TSession::TPtr session);
    TVoid OnSessionFinished(TVoid, TSession::TPtr session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

