#include "session.h"

#include "../misc/fs.h"
#include "../misc/assert.h"

#include <util/generic/yexception.h>

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
    , LeaseManager(New<TLeaseManager>())
{ }

TSession::TPtr TSessionManager::FindSession(const TChunkId& chunkId)
{
    auto it = SessionMap.find(chunkId);
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
    TLocation::TPtr location = ChunkStore->GetNewChunkLocation();

    auto session = New<TSession>(this, chunkId, location, windowSize);

    TLeaseManager::TLease lease = LeaseManager->CreateLease(
        Config.SessionTimeout,
        FromMethod(
            &TSessionManager::OnLeaseExpired,
            TPtr(this),
            session)
        ->Via(ServiceInvoker));
    session->SetLease(lease);

    YVERIFY(SessionMap.insert(MakePair(chunkId, session)).Second());

    LOG_INFO("Session started (ChunkId: %s, Location: %s, WindowSize: %d)",
        ~chunkId.ToString(),
        ~location->GetPath(),
        windowSize);

    return session;
}

void TSessionManager::CancelSession(TSession::TPtr session)
{
    TChunkId chunkId = session->GetChunkId();

    YVERIFY(SessionMap.erase(chunkId) == 1);

    session->Cancel();

    LOG_INFO("Session canceled (ChunkId: %s)",
        ~chunkId.ToString());
}

TFuture<TVoid>::TPtr TSessionManager::FinishSession(TSession::TPtr session)
{
    TChunkId chunkId = session->GetChunkId();

    YVERIFY(SessionMap.erase(chunkId) == 1);

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
        ~session->GetChunkId().ToString());

    return TVoid();
}

void TSessionManager::OnLeaseExpired(TSession::TPtr session)
{
    if (SessionMap.find(session->GetChunkId()) != SessionMap.end()) {
        LOG_INFO("Session lease expired (ChunkId: %s)",
            ~session->GetChunkId().ToString());

        CancelSession(session);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
    TSessionManager::TPtr sessionManager,
    const TChunkId& chunkId,
    TLocation::TPtr location,
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
    return Location->GetInvoker();
}

TChunkId TSession::GetChunkId() const
{
    return ChunkId;
}

TLocation::TPtr TSession::GetLocation() const
{
    return Location;
}

i64 TSession::GetSize() const
{
    return Size;
}

TCachedBlock::TPtr TSession::GetBlock(i32 blockIndex)
{
    VerifyInWindow(blockIndex);

    RenewLease();

    const TSlot& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        ythrow TServiceException(EErrorCode::WindowError) <<
            Sprintf("retrieving a block that is not received (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
                ~ChunkId.ToString(),
                WindowStart,
                Window.ysize(),
                blockIndex);
    }

    LOG_DEBUG("Chunk block retrieved (ChunkId: %s, BlockIndex: %d)",
        ~ChunkId.ToString(),
        blockIndex);

    return slot.Block;
}

void TSession::PutBlock(i32 blockIndex, const TSharedRef& data)
{
    TBlockId blockId(ChunkId, blockIndex);

    VerifyInWindow(blockIndex);

    RenewLease();

    TSlot& slot = GetSlot(blockIndex);
    if (slot.State != ESlotState::Empty) {
        if (CompareMemory(slot.Block->GetData(), data)) {
            LOG_WARNING("Block has been already received (BlockId: %s)",
                ~blockId.ToString());
            return;
        }

        ythrow TServiceException(EErrorCode::UnmatchedBlockContent) <<
            Sprintf("block with the same blockId but different content already received (BlockId: %s, WindowStart: %d, WindowSize: %d)",
            ~blockId.ToString(),
            WindowStart,
            Window.ysize());
    }

    slot.State = ESlotState::Received;
    slot.Block = SessionManager->BlockStore->PutBlock(blockId, data);

    Size += data.Size();

    LOG_DEBUG("Chunk block received (BlockId: %s)", ~blockId.ToString());

    EnqueueWrites();
}

void TSession::EnqueueWrites()
{
    while (IsInWindow(FirstUnwritten)) {
        i32 blockIndex = FirstUnwritten;

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

TVoid TSession::DoWrite(TCachedBlock::TPtr block, i32 blockIndex)
{
    LOG_DEBUG("Start writing chunk block (ChunkId: %s, BlockIndex: %d)",
        ~ChunkId.ToString(),
        blockIndex);

    try {
        Writer->WriteBlock(block->GetData());
    } catch (...) {
        LOG_FATAL("Error writing chunk block  (ChunkId: %s, BlockIndex: %d, What: %s)",
            ~ChunkId.ToString(),
            blockIndex,
            ~CurrentExceptionMessage());
    }

    LOG_DEBUG("Chunk block written (ChunkId: %s, BlockIndex: %d)",
        ~ChunkId.ToString(),
        blockIndex);

    return TVoid();
}

void TSession::OnBlockWritten(TVoid, i32 blockIndex)
{
    TSlot& slot = GetSlot(blockIndex);
    YASSERT(slot.State == ESlotState::Received);
    slot.State = ESlotState::Written;
    slot.IsWritten->Set(TVoid());
}

TFuture<TVoid>::TPtr TSession::FlushBlock(i32 blockIndex)
{
    // TODO: verify monotonicity of blockIndex

    VerifyInWindow(blockIndex);

    RenewLease();

    const TSlot& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        ythrow TServiceException(EErrorCode::WindowError) <<
            Sprintf("flushing an empty block (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
                ~ChunkId.ToString(),
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

TVoid TSession::OnBlockFlushed(TVoid, i32 blockIndex)
{
    RotateWindow(blockIndex);
    return TVoid();
}

TFuture<TVoid>::TPtr TSession::Finish()
{
    CloseLease();

    for (i32 blockIndex = WindowStart; blockIndex < WindowStart + Window.ysize(); ++blockIndex) {
        const TSlot& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            ythrow TServiceException(EErrorCode::WindowError) <<
                Sprintf("finishing a session with an unflushed block (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
                    ~ChunkId.ToString(),
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
    Writer = New<TFileChunkWriter>(FileName + NFS::TempFileSuffix);

    LOG_DEBUG("Chunk file opened (ChunkId: %s)",
        ~ChunkId.ToString());
}

void TSession::DeleteFile()
{
    GetInvoker()->Invoke(FromMethod(
        &TSession::DoDeleteFile,
        TPtr(this)));
}

void TSession::DoDeleteFile()
{
    Writer->Cancel();

    if (!NFS::Remove(FileName + NFS::TempFileSuffix)) {
        LOG_FATAL("Error deleting chunk file (ChunkId: %s)",
            ~ChunkId.ToString());
    }

    LOG_DEBUG("Chunk file deleted (ChunkId: %s)",
        ~ChunkId.ToString());
}

TFuture<TVoid>::TPtr TSession::CloseFile()
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
    try {
        Writer->Close();
    } catch (...) {
        LOG_FATAL("Error flushing chunk file (ChunkId: %s)",
            ~ChunkId.ToString());
    }

    if (!NFS::Rename(FileName + NFS::TempFileSuffix, FileName)) {
        LOG_FATAL("Error renaming temp chunk file (ChunkId: %s)",
            ~ChunkId.ToString());
    }

    LOG_DEBUG("Chunk file closed (ChunkId: %s)",
        ~ChunkId.ToString());

    return TVoid();
}

void TSession::RotateWindow(i32 flushedBlockIndex)
{
    YASSERT(WindowStart <= flushedBlockIndex);

    while (WindowStart <= flushedBlockIndex) {
        GetSlot(WindowStart) = TSlot();
        ++WindowStart;
    }

    LOG_DEBUG("Window rotated (ChunkId: %s, WindowStart: %d)",
        ~ChunkId.ToString(),
        WindowStart);
}

bool TSession::IsInWindow(i32 blockIndex)
{
    return blockIndex >= WindowStart && blockIndex < WindowStart + Window.ysize();
}

void TSession::VerifyInWindow(i32 blockIndex)
{
    if (!IsInWindow(blockIndex)) {
        ythrow TServiceException(EErrorCode::WindowError) <<
            Sprintf("accessing a block out of the window (ChunkId: %s, WindowStart: %d, WindowSize: %d, BlockIndex: %d)",
                ~ChunkId.ToString(),
                WindowStart,
                Window.ysize(),
                blockIndex);
    }
}

TSession::TSlot& TSession::GetSlot(i32 blockIndex)
{
    YASSERT(IsInWindow(blockIndex));
    return Window[blockIndex % Window.ysize()];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
