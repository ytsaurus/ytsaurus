#include "stdafx.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "block_store.h"
#include "chunk.h"
#include "chunk_store.h"
#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/chunk_client/file_writer.h>

#include <ytlib/misc/fs.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
    TSessionManagerPtr sessionManager,
    const TChunkId& chunkId,
    TLocationPtr location)
    : SessionManager(sessionManager)
    , ChunkId(chunkId)
    , Location(location)
    , WindowStart(0)
    , FirstUnwritten(0)
    , Size(0)
    , Logger(ChunkHolderLogger)
{
    YASSERT(sessionManager);
    YASSERT(location);

    Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));

    Location->UpdateSessionCount(+1);
    FileName = Location->GetChunkFileName(ChunkId);
}

TSession::~TSession()
{
    Location->UpdateSessionCount(-1);
}

void TSession::Start()
{
    GetIOInvoker()->Invoke(BIND(&TSession::DoOpenFile, MakeStrong(this)));
}

void TSession::DoOpenFile()
{
    try {
        NFS::ForcePath(NFS::GetDirectoryName(FileName));
        Writer = New<TFileWriter>(FileName);
        Writer->Open();
    }
    catch (const std::exception& ex) {
        LOG_FATAL("Error opening chunk file\n%s", ex.what());
    }
    LOG_DEBUG("Chunk file opened");
}

void TSession::SetLease(TLeaseManager::TLease lease)
{
    Lease = lease;
}

void TSession::RenewLease()
{
    TLeaseManager::RenewLease(Lease);
}

void TSession::CloseLease()
{
    TLeaseManager::CloseLease(Lease);
}

IInvokerPtr TSession::GetIOInvoker()
{
    return Location->GetInvoker();
}

TChunkId TSession::GetChunkId() const
{
    return ChunkId;
}

TLocationPtr TSession::GetLocation() const
{
    return Location;
}

i64 TSession::GetSize() const
{
    return Size;
}

TChunkInfo TSession::GetChunkInfo() const
{
    return Writer->GetChunkInfo();
}

TCachedBlockPtr TSession::GetBlock(i32 blockIndex)
{
    VerifyInWindow(blockIndex);

    RenewLease();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        ythrow TServiceException(EErrorCode::WindowError) <<
            Sprintf("Trying to retrieve a block %d that is not received yet (WindowStart: %d)",
            blockIndex,
            WindowStart);
    }

    LOG_DEBUG("Chunk block %d retrieved", blockIndex);

    return slot.Block;
}

void TSession::PutBlock(i32 blockIndex, const TSharedRef& data)
{
    TBlockId blockId(ChunkId, blockIndex);

    VerifyInWindow(blockIndex);

    RenewLease();

    if (!Location->HasEnoughSpace(data.Size())) {
        ythrow TServiceException(EErrorCode::OutOfSpace) <<
            "No enough space left on node";
    }

    auto& slot = GetSlot(blockIndex);
    if (slot.State != ESlotState::Empty) {
        return;
        // TODO(babenko): switched off for now
        //if (TRef::CompareContent(slot.Block->GetData(), data)) {
        //    LOG_WARNING("Block %d is already received", blockIndex);
        //    return;
        //}

        //ythrow TServiceException(EErrorCode::BlockContentMismatch) <<
        //    Sprintf("Block %d with a different content already received (WindowStart: %d)",
        //        blockIndex,
        //        WindowStart);
    }

    slot.State = ESlotState::Received;
    slot.Block = SessionManager->BlockStore->PutBlock(blockId, data, Stroka());

    Location->UpdateUsedSpace(data.Size());
    Size += data.Size();

    LOG_DEBUG("Chunk block %d received", blockIndex);

    EnqueueWrites();
}

void TSession::EnqueueWrites()
{
    while (IsInWindow(FirstUnwritten)) {
        i32 blockIndex = FirstUnwritten;

        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Received)
            break;

        BIND(
            &TSession::DoWrite,
            MakeStrong(this),
            slot.Block,
            blockIndex)
        .AsyncVia(GetIOInvoker())
        .Run()
        .Subscribe(BIND(
            &TSession::OnBlockWritten,
            MakeStrong(this),
            blockIndex)
        .Via(SessionManager->ServiceInvoker));

        ++FirstUnwritten;
    }
}

TVoid TSession::DoWrite(TCachedBlockPtr block, i32 blockIndex)
{
    LOG_DEBUG("Start writing chunk block %d",
        blockIndex);

    try {
        if (!Writer->TryWriteBlock(block->GetData())) {
            Sync(~Writer, &TFileWriter::GetReadyEvent);
            YUNREACHABLE();
        }
    } catch (const std::exception& ex) {
        LOG_FATAL("Error writing chunk block %d\n%s",
            blockIndex,
            ex.what());
    }

    LOG_DEBUG("Chunk block %d written", blockIndex);

    return TVoid();
}

void TSession::OnBlockWritten(i32 blockIndex, TVoid)
{
    auto& slot = GetSlot(blockIndex);
    YASSERT(slot.State == ESlotState::Received);
    slot.State = ESlotState::Written;
    slot.IsWritten.Set(TVoid());
}

TFuture<void> TSession::FlushBlock(i32 blockIndex)
{
    // TODO: verify monotonicity of blockIndex

    VerifyInWindow(blockIndex);

    RenewLease();

    const TSlot& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        ythrow TServiceException(EErrorCode::WindowError) <<
            Sprintf("Attempt to flush an unreceived block %d (WindowStart: %d, WindowSize: %d)",
            blockIndex,
            WindowStart,
            Window.ysize());
    }

    // IsWritten is set in ServiceInvoker, hence no need for AsyncVia.
    return slot.IsWritten.ToFuture().Apply(BIND(
        &TSession::OnBlockFlushed,
        MakeStrong(this),
        blockIndex));
}

void TSession::OnBlockFlushed(i32 blockIndex, TVoid)
{
    ReleaseBlocks(blockIndex);
    return;
}

TFuture<TChunkPtr> TSession::Finish(const TChunkMeta& chunkMeta)
{
    CloseLease();

    for (i32 blockIndex = WindowStart; blockIndex < Window.ysize(); ++blockIndex) {
        const TSlot& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            ythrow TServiceException(EErrorCode::WindowError) <<
                Sprintf("Attempt to finish a session with an unflushed block %d (WindowStart: %d, WindowSize: %d)",
                blockIndex,
                WindowStart,
                Window.ysize());
        }
    }

    return CloseFile(chunkMeta).Apply(
        BIND(&TSession::OnFileClosed, MakeStrong(this))
        .AsyncVia(SessionManager->ServiceInvoker));
}

void TSession::Cancel(const TError& error)
{
    CloseLease();
    DeleteFile(error)
        .Apply(BIND(&TSession::OnFileDeleted, MakeStrong(this))
        .AsyncVia(SessionManager->ServiceInvoker));
}

TFuture<TVoid> TSession::DeleteFile(const TError& error)
{
    return
        BIND(&TSession::DoDeleteFile, MakeStrong(this), error)
        .AsyncVia(GetIOInvoker())
        .Run();
}

TVoid TSession::DoDeleteFile(const TError& error)
{
    Writer->Abort();
    Writer.Reset();

    LOG_DEBUG("Chunk file deleted\n%s", ~error.ToString());

    return TVoid();
}

TVoid TSession::OnFileDeleted(TVoid)
{
    ReleaseSpaceOccupiedByBlocks();
    return TVoid();
}

TFuture<TVoid> TSession::CloseFile(const TChunkMeta& chunkMeta)
{
    return
        BIND(&TSession::DoCloseFile,MakeStrong(this), chunkMeta)
        .AsyncVia(GetIOInvoker())
        .Run();
}

TVoid TSession::DoCloseFile(const TChunkMeta& chunkMeta)
{
    try {
        Sync(~Writer, &TFileWriter::AsyncClose, chunkMeta);
    } catch (const std::exception& ex) {
        LOG_FATAL("Error closing chunk file\n%s", ex.what());
    }

    LOG_DEBUG("Chunk file closed");

    return TVoid();
}

TChunkPtr TSession::OnFileClosed(TVoid)
{
    ReleaseSpaceOccupiedByBlocks();
    auto chunk = New<TStoredChunk>(
        ~Location, 
        ChunkId, 
        Writer->GetChunkMeta(), 
        Writer->GetChunkInfo());
    SessionManager->ChunkStore->RegisterChunk(~chunk);
    return chunk;
}

void TSession::ReleaseBlocks(i32 flushedBlockIndex)
{
    YASSERT(WindowStart <= flushedBlockIndex);

    while (WindowStart <= flushedBlockIndex) {
        auto& slot = GetSlot(WindowStart);
        YASSERT(slot.State == ESlotState::Written);
        slot.Block.Reset();
        slot.IsWritten.Reset();
        ++WindowStart;
    }

    LOG_DEBUG("Released blocks (WindowStart: %d)",
        WindowStart);
}

bool TSession::IsInWindow(i32 blockIndex)
{
    return blockIndex >= WindowStart;
}

void TSession::VerifyInWindow(i32 blockIndex)
{
    if (!IsInWindow(blockIndex)) {
        ythrow TServiceException(EErrorCode::WindowError) <<
            Sprintf("Block %d is out of the window (WindowStart: %d, WindowSize: %d)",
                blockIndex,
                WindowStart,
                Window.ysize());
    }
}

TSession::TSlot& TSession::GetSlot(i32 blockIndex)
{
    YASSERT(IsInWindow(blockIndex));
    if (Window.size() <= blockIndex)
        Window.reserve(blockIndex + 1);

    while (Window.size() <= blockIndex) {
        // NB: do not use resize here! 
        // Newly added slots must get a fresh copy of IsWritten promise.
        // Using resize would cause all of these slots to share a single promise.
        Window.push_back(TSlot());
    }

    if (Window.size() <= blockIndex) {
        Window.resize(1 + blockIndex);
    }

    return Window[blockIndex];
}

void TSession::ReleaseSpaceOccupiedByBlocks()
{
    Location->UpdateUsedSpace(-Size);
}

////////////////////////////////////////////////////////////////////////////////

TSessionManager::TSessionManager(
    TChunkHolderConfigPtr config,
    TBlockStorePtr blockStore,
    TChunkStorePtr chunkStore,
    IInvokerPtr serviceInvoker)
    : Config(config)
    , BlockStore(blockStore)
    , ChunkStore(chunkStore)
    , ServiceInvoker(serviceInvoker)
{
    YASSERT(blockStore);
    YASSERT(chunkStore);
    YASSERT(serviceInvoker);
}

TSessionPtr TSessionManager::FindSession(const TChunkId& chunkId) const
{
    auto it = SessionMap.find(chunkId);
    if (it == SessionMap.end())
        return NULL;
    
    auto session = it->second;
    session->RenewLease();
    return session;
}

TSessionPtr TSessionManager::StartSession(
    const TChunkId& chunkId)
{
    auto location = ChunkStore->GetNewChunkLocation();

    auto session = New<TSession>(this, chunkId, ~location);
    session->Start();

    auto lease = TLeaseManager::CreateLease(
        Config->SessionTimeout,
        BIND(
            &TSessionManager::OnLeaseExpired,
            MakeStrong(this),
            session)
        .Via(ServiceInvoker));
    session->SetLease(lease);

    AtomicIncrement(SessionCount);
    YCHECK(SessionMap.insert(MakePair(chunkId, session)).second);

    LOG_INFO("Session %s started at %s",
        ~chunkId.ToString(),
        ~location->GetPath().Quote());
    return session;
}

void TSessionManager::CancelSession(TSessionPtr session, const TError& error)
{
    auto chunkId = session->GetChunkId();

    YCHECK(SessionMap.erase(chunkId) == 1);
    AtomicDecrement(SessionCount);

    session->Cancel(error);

    LOG_INFO("Session %s canceled\n%s",
        ~chunkId.ToString(),
        ~error.ToString());
}

TFuture<TChunkPtr> TSessionManager::FinishSession(
    TSessionPtr session, 
    const TChunkMeta& chunkMeta)
{
    auto chunkId = session->GetChunkId();

    YCHECK(SessionMap.erase(chunkId) == 1);
    AtomicDecrement(SessionCount);

    return session
        ->Finish(chunkMeta)
        .Apply(BIND(
            &TSessionManager::OnSessionFinished,
            MakeStrong(this),
            session));
}

TChunkPtr TSessionManager::OnSessionFinished(TSessionPtr session, TChunkPtr chunk)
{
    LOG_INFO("Session %s finished", ~session->GetChunkId().ToString());
    return chunk;
}

void TSessionManager::OnLeaseExpired(TSessionPtr session)
{
    if (SessionMap.find(session->GetChunkId()) != SessionMap.end()) {
        LOG_INFO("Session %s lease expired", ~session->GetChunkId().ToString());
        CancelSession(~session, TError("Session lease expired"));
    }
}

int TSessionManager::GetSessionCount() const
{
    return SessionCount;
}

TSessionManager::TSessions TSessionManager::GetSessions() const
{
    TSessions result;
    YCHECK(SessionMap.ysize() == SessionCount);
    result.reserve(SessionMap.ysize());
    FOREACH (const auto& pair, SessionMap) {
        result.push_back(pair.second);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NChunkHolder
} // namespace NYT
