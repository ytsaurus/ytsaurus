#include "session.h"

#include "../misc/fs.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TSessionManager::TSessionManager(
    const TChunkHolderConfig& config,
    TBlockStore::TPtr blockStore,
    TChunkStore::TPtr chunkStore,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , BlockStore(blockStore)
    , ChunkStore(chunkStore)
    , ServiceInvoker(serviceInvoker)
    , LeaseManager(new TLeaseManager())
{ }

TSession::TPtr TSessionManager::FindSession(const TChunkId& chunkId)
{
    TSessionMap::iterator it = SessionMap.find(chunkId);
    if (it == SessionMap.end())
        return NULL;
    
    TSession::TPtr session = it->Second();
    session->RenewLease();
    return session;
}

TSession::TPtr TSessionManager::StartSession(
    const TChunkId& chunkId,
    int windowSize)
{
    int location = ChunkStore->GetNewChunkLocation(chunkId);

    TSession::TPtr session = new TSession(this, chunkId, location, windowSize);
    TLeaseManager::TLease lease = LeaseManager->CreateLease(
        Config.LeaseTimeout,
        FromMethod(
            &TSessionManager::OnLeaseExpired,
            TPtr(this),
            session)
        ->Via(ServiceInvoker));
    session->SetLease(lease);

    // TODO: use YVERIFY
    VERIFY(SessionMap.insert(MakePair(chunkId, session)).Second(), "oops");

    LOG_INFO("Session started (ChunkId: %s, Location: %d, WindowSize: %d)",
        ~StringFromGuid(chunkId),
        location,
        windowSize);

    return session;
}

void TSessionManager::CancelSession(TSession::TPtr session)
{
    TChunkId chunkId = session->GetChunkId();

    // TODO: use YVERIFY
    VERIFY(SessionMap.erase(chunkId) == 1, "oops");

    session->Cancel();

    LOG_INFO("Session canceled (ChunkId: %s)",
        ~StringFromGuid(chunkId));
}

TAsyncResult<TVoid>::TPtr TSessionManager::FinishSession(TSession::TPtr session)
{
    TChunkId chunkId = session->GetChunkId();

    // TODO: use YVERIFY
    VERIFY(SessionMap.erase(chunkId) == 1, "oops");

    return session->Finish()->Apply(
        FromMethod(
            &TSessionManager::OnSessionFinished,
            TPtr(this),
            session)
        ->AsyncVia(ServiceInvoker));
}

TVoid TSessionManager::OnSessionFinished(TVoid, TSession::TPtr session)
{
    ChunkStore->RegisterChunk(
        session->GetChunkId(),
        session->GetSize(),
        session->GetLocation());

    LOG_INFO("Session finished (ChunkId: %s)",
        ~StringFromGuid(session->GetChunkId()));

    return TVoid();
}

void TSessionManager::OnLeaseExpired(TSession::TPtr session)
{
    if (SessionMap.find(session->GetChunkId()) != SessionMap.end()) {
        LOG_INFO("Session lease expired (ChunkId: %s)",
            ~StringFromGuid(session->GetChunkId()));

        CancelSession(session);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
    TSessionManager::TPtr sessionManager,
    const TChunkId& chunkId,
    int location,
    int windowSize)
    : SessionManager(sessionManager)
    , ChunkId(chunkId)
    , Location(location)
    , WindowStart(0)
    , FirstUnwritten(0)
    , Size(0)
{
    FileName = SessionManager->ChunkStore->GetChunkFileName(chunkId, location);

    for (int index = 0; index < windowSize; ++index) {
        Window.push_back(TSlot());
    }

    OpenFile();
}

void TSession::SetLease(TLeaseManager::TLease lease)
{
    Lease = lease;
}

void TSession::RenewLease()
{
    SessionManager->LeaseManager->RenewLease(Lease);
}

void TSession::CloseLease()
{
    SessionManager->LeaseManager->CloseLease(Lease);
}

IInvoker::TPtr TSession::GetInvoker()
{
    return SessionManager->ChunkStore->GetIOInvoker(Location);
}

TChunkId TSession::GetChunkId() const
{
    return ChunkId;
}

int TSession::GetLocation() const
{
    return Location;
}

i64 TSession::GetSize() const
{
    return Size;
}

TCachedBlock::TPtr TSession::GetBlock(int blockIndex)
{
    VerifyInWindow(blockIndex);

    RenewLease();

    const TSlot& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        ythrow TServiceException(TErrorCode::WindowError)
            << Sprintf("retrieving a block that is not received (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
            ~StringFromGuid(ChunkId),
            WindowStart,
            Window.ysize(),
            blockIndex);
    }

    LOG_DEBUG("Chunk block retrieved (ChunkId: %s, BlockIndex: %d)",
        ~StringFromGuid(ChunkId),
        blockIndex);

    return slot.Block;
}

void TSession::PutBlock(
    i32 blockIndex,
    const TBlockId& blockId,
    const TSharedRef& data)
{
    VerifyInWindow(blockIndex);

    RenewLease();

    TSlot& slot = GetSlot(blockIndex);
    if (slot.State != ESlotState::Empty) {
        ythrow TServiceException(TErrorCode::WindowError)
            << Sprintf("putting an already received block received (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
            ~StringFromGuid(ChunkId),
            WindowStart,
            Window.ysize(),
            blockIndex);
    }

    slot.State = ESlotState::Received;
    slot.Block = SessionManager->BlockStore->PutBlock(blockId, data);
    Size += data.Size();

    LOG_DEBUG("Chunk block received (ChunkId: %s, BlockIndex: %d, BlockId: %s, BlockSize: %d)",
        ~StringFromGuid(ChunkId),
        blockIndex,
        ~blockId.ToString(),
        data.Size());

    EnqueueWrites();
}

void TSession::EnqueueWrites()
{
    while (IsInWindow(FirstUnwritten)) {
        int blockIndex = FirstUnwritten;

        const TSlot& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Received)
            break;
        
        FromMethod(
            &TSession::DoWrite,
            TPtr(this),
            slot.Block,
            blockIndex)
        ->AsyncVia(GetInvoker())
        ->Do()
        ->Subscribe(FromMethod(
            &TSession::OnBlockWritten,
            TPtr(this),
            blockIndex)
        ->Via(SessionManager->ServiceInvoker));

        ++FirstUnwritten;
    }
}

TVoid TSession::DoWrite(TCachedBlock::TPtr block, int blockIndex)
{
    // TODO: IO exceptions
    File->Write(block->GetData().Begin(), block->GetData().Size());
    File->Flush();

    LOG_DEBUG("Chunk block written (ChunkId: %s, BlockIndex: %d)",
        ~StringFromGuid(ChunkId),
        blockIndex);

    return TVoid();
}

void TSession::OnBlockWritten(TVoid, int blockIndex)
{
    TSlot& slot = GetSlot(blockIndex);
    YASSERT(slot.State == ESlotState::Received);
    slot.State = ESlotState::Written;
    slot.IsWritten->Set(TVoid());
}

TAsyncResult<TVoid>::TPtr TSession::FlushBlock(int blockIndex)
{
    VerifyInWindow(blockIndex);

    RenewLease();

    const TSlot& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        ythrow TServiceException(TErrorCode::WindowError)
            << Sprintf("flushing an empty block (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
            ~StringFromGuid(ChunkId),
            WindowStart,
            Window.ysize(),
            blockIndex);
    }

    // IsWritten is set in ServiceInvoker, hence no need for AsyncVia.
    return slot.IsWritten->Apply(FromMethod(
            &TSession::OnBlockFlushed,
            TPtr(this),
            blockIndex));
}

TVoid TSession::OnBlockFlushed(TVoid, int blockIndex)
{
    RotateWindow(blockIndex);
    return TVoid();
}

TAsyncResult<TVoid>::TPtr TSession::Finish()
{
    CloseLease();

    for (int blockIndex = WindowStart; blockIndex < WindowStart + Window.ysize(); ++blockIndex) {
        const TSlot& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            ythrow TServiceException(TErrorCode::WindowError)
                << Sprintf("finishing a session with an unflushed block (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
                ~StringFromGuid(ChunkId),
                WindowStart,
                Window.ysize(),
                blockIndex);
        }
    }

    return CloseFile();
}

void TSession::Cancel()
{
    CloseLease();
    DeleteFile();
}

void TSession::OpenFile()
{
    GetInvoker()->Invoke(FromMethod(
        &TSession::DoOpenFile,
        TPtr(this)));
}

void TSession::DoOpenFile()
{
    File.Reset(new TFile(FileName + NFS::TempFileSuffix, CreateAlways/*|WrOnly|Seq|Direct*/));

    LOG_DEBUG("Chunk file opened (ChunkId: %s)",
        ~StringFromGuid(ChunkId));
}

void TSession::DeleteFile()
{
    GetInvoker()->Invoke(FromMethod(
        &TSession::DoDeleteFile,
        TPtr(this)));
}

void TSession::DoDeleteFile()
{
    File.Destroy();

    // TODO: check error code
    NFS::Remove(FileName + NFS::TempFileSuffix);

    LOG_DEBUG("Chunk file deleted (ChunkId: %s)",
        ~StringFromGuid(ChunkId));
}

TAsyncResult<TVoid>::TPtr TSession::CloseFile()
{
    return
        FromMethod(
            &TSession::DoCloseFile,
            TPtr(this))
        ->AsyncVia(GetInvoker())
        ->Do();
}

TVoid TSession::DoCloseFile()
{
    // TODO: IO exceptions
    File->Flush();
    File.Destroy();

    // TODO: check error code
    NFS::Rename(FileName + NFS::TempFileSuffix, FileName);

    LOG_DEBUG("Chunk file closed (ChunkId: %s)",
        ~StringFromGuid(ChunkId));

    return TVoid();
}

void TSession::RotateWindow(int flushedBlockIndex)
{
    YASSERT(WindowStart <= flushedBlockIndex);

    while (WindowStart <= flushedBlockIndex) {
        GetSlot(WindowStart) = TSlot();
        ++WindowStart;
    }

    LOG_DEBUG("Window rotated (ChunkId: %s, WindowStart: %d)",
        ~StringFromGuid(ChunkId),
        WindowStart);
}

bool TSession::IsInWindow(int blockIndex)
{
    return blockIndex >= WindowStart && blockIndex < WindowStart + Window.ysize();
}

void TSession::VerifyInWindow(int blockIndex)
{
    if (!IsInWindow(blockIndex)) {
        ythrow TServiceException(TErrorCode::WindowError)
            << Sprintf("accessing a block out of the window (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
            ~StringFromGuid(ChunkId),
            WindowStart,
            Window.ysize(),
            blockIndex);
    }
}

TSession::TSlot& TSession::GetSlot(int index)
{
    YASSERT(IsInWindow(index));
    return Window[index % Window.ysize()];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
