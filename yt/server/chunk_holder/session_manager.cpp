#include "stdafx.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "block_store.h"
#include "chunk.h"
#include "chunk_store.h"
#include "bootstrap.h"

#include <ytlib/chunk_client/chunk.pb.h>
#include <ytlib/chunk_client/file_writer.h>

#include <ytlib/misc/fs.h>
#include <ytlib/misc/sync.h>

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;
static NProfiling::TProfiler& Profiler = DataNodeProfiler;

static NProfiling::TRateCounter WriteThroughputCounter("/chunk_io/write_throughput");

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    TLocationPtr location)
    : Config(config)
    , Bootstrap(bootstrap)
    , ChunkId(chunkId)
    , Location(location)
    , WindowStartIndex(0)
    , WriteIndex(0)
    , Size(0)
    , WriteInvoker(CreateSerializedInvoker(Location->GetWriteInvoker()))
    , Logger(DataNodeLogger)
{
    YCHECK(bootstrap);
    YCHECK(location);

    Logger.AddTag(Sprintf("LocationId: %s, ChunkId: %s",
        ~Location->GetId(),
        ~ChunkId.ToString()));

    Location->UpdateSessionCount(+1);
    FileName = Location->GetChunkFileName(ChunkId);
}

TSession::~TSession()
{
    Location->UpdateSessionCount(-1);
}

void TSession::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Session started");

    WriteInvoker->Invoke(BIND(&TSession::DoOpenFile, MakeStrong(this)));
}

void TSession::DoOpenFile()
{
    LOG_DEBUG("Started opening chunk writer");
    
    PROFILE_TIMING ("/chunk_io/chunk_writer_open_time") {
        try {
            Writer = New<TFileWriter>(FileName);
            Writer->Open();
        }
        catch (const std::exception& ex) {
            OnIOError(TError(
                TDataNodeServiceProxy::EErrorCode::IOError,
                "Error creating chunk: %s",
                ~ToString(ChunkId))
                << ex);
            return;
        }
    }

    LOG_DEBUG("Finished opening chunk writer");
}

void TSession::SetLease(TLeaseManager::TLease lease)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Lease = lease;
}

void TSession::RenewLease()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    TLeaseManager::RenewLease(Lease);
}

void TSession::CloseLease()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    TLeaseManager::CloseLease(Lease);
}

TChunkId TSession::GetChunkId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ChunkId;
}

TLocationPtr TSession::GetLocation() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Location;
}

i64 TSession::GetSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Size;
}

int TSession::GetWrittenBlockCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return WindowStartIndex;
}

TChunkInfo TSession::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Writer->GetChunkInfo();
}

void TSession::PutBlock(
    int blockIndex,
    const TSharedRef& data,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TBlockId blockId(ChunkId, blockIndex);

    VerifyInWindow(blockIndex);
    RenewLease();

    if (!Location->HasEnoughSpace(data.Size())) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::OutOfSpace,
            "No enough space left on node");
    }

    auto& slot = GetSlot(blockIndex);
    if (slot.State != ESlotState::Empty) {
        if (TRef::CompareContent(slot.Block, data)) {
            LOG_WARNING("Block %d is already received", blockIndex);
            return;
        }

        THROW_ERROR_EXCEPTION(
            EErrorCode::BlockContentMismatch,
            "Block %d with a different content already received (WindowStart: %d)",
            blockIndex,
            WindowStartIndex);
    }

    slot.State = ESlotState::Received;
    slot.Block = data;

    if (enableCaching) {
        Bootstrap->GetBlockStore()->PutBlock(blockId, data, Null);
    }

    Location->UpdateUsedSpace(data.Size());
    Size += data.Size();

    LOG_DEBUG("Chunk block %d received", blockIndex);

    EnqueueWrites();
}

TAsyncError TSession::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const Stroka& targetAddress)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TProxy proxy(ChannelCache.GetChannel(targetAddress));
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    auto req = proxy.PutBlocks();
    *req->mutable_chunk_id() = ChunkId.ToProto();
    req->set_start_block_index(startBlockIndex);

    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = GetBlock(blockIndex);
        req->Attachments().push_back(block);
    }

    return req->Invoke().Apply(BIND([=] (TProxy::TRspPutBlocksPtr rsp) {
        return rsp->GetError();
    }));
}

void TSession::EnqueueWrites()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    while (WriteIndex < Window.size()) {
        const auto& slot = GetSlot(WriteIndex);
        YCHECK(slot.State == ESlotState::Received || slot.State == ESlotState::Empty);
        if (slot.State == ESlotState::Empty)
            break;

        BIND(
            &TSession::DoWriteBlock,
            MakeStrong(this),
            slot.Block, 
            WriteIndex)
        .AsyncVia(WriteInvoker)
        .Run()
        .Subscribe(BIND(
            &TSession::OnBlockWritten,
            MakeStrong(this),
            WriteIndex)
        .Via(Bootstrap->GetControlInvoker()));
        ++WriteIndex;
    }
}

TError TSession::DoWriteBlock(const TSharedRef& block, int blockIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error.IsOK()) {
        return Error;
    }

    LOG_DEBUG("Started writing block %d", blockIndex);

    PROFILE_TIMING ("/chunk_io/block_write_time") {
        try {
            if (!Writer->TryWriteBlock(block)) {
                // This will throw...
                Sync(~Writer, &TFileWriter::GetReadyEvent);
                // ... so we never get here.
                YUNREACHABLE();
            }
        } catch (const std::exception& ex) {
            TBlockId blockId(ChunkId, blockIndex);
            OnIOError(TError(
                TDataNodeServiceProxy::EErrorCode::IOError,
                "Error writing chunk block: %s",
                ~blockId.ToString())
                << ex);
        }
    }

    LOG_DEBUG("Finished writing block %d", blockIndex);

    Profiler.Enqueue("/chunk_io/block_write_size", block.Size());
    Profiler.Increment(WriteThroughputCounter, block.Size());

    return Error;
}

void TSession::OnBlockWritten(int blockIndex, TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    UNUSED(error);

    auto& slot = GetSlot(blockIndex);
    YCHECK(slot.State == ESlotState::Received);
    slot.State = ESlotState::Written;
    slot.IsWritten.Set();
}

TAsyncError TSession::FlushBlock(int blockIndex)
{
    // TODO: verify monotonicity of blockIndex
    VERIFY_THREAD_AFFINITY(ControlThread);

    VerifyInWindow(blockIndex);
    RenewLease();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::WindowError,
            "Attempt to flush an unreceived block %d (WindowStart: %d, WindowSize: %" PRISZT ")",
            blockIndex,
            WindowStartIndex,
            Window.size());
    }

    // IsWritten is set in the control thread, hence no need for AsyncVia.
    return slot.IsWritten.ToFuture().Apply(BIND(
        &TSession::OnBlockFlushed,
        MakeStrong(this),
        blockIndex));
}

TError TSession::OnBlockFlushed(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseBlocks(blockIndex);
    return Error;
}

TFuture< TValueOrError<TChunkPtr> > TSession::Finish(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CloseLease();

    for (int blockIndex = WindowStartIndex; blockIndex < Window.size(); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::WindowError,
                "Attempt to finish a session with an unflushed block %d (WindowStart: %d, WindowSize: %" PRISZT ")",
                blockIndex,
                WindowStartIndex,
                Window.size());
        }
    }

    LOG_DEBUG("Session finished");

    return CloseFile(chunkMeta).Apply(
        BIND(&TSession::OnFileClosed, MakeStrong(this))
        .AsyncVia(Bootstrap->GetControlInvoker()));
}

void TSession::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG(error, "Session canceled");

    CloseLease();
    AbortWriter()
        .Apply(BIND(&TSession::OnWriterAborted, MakeStrong(this))
        .AsyncVia(Bootstrap->GetControlInvoker()));
}

TAsyncError TSession::AbortWriter()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return
        BIND(&TSession::DoAbortWriter, MakeStrong(this))
        .AsyncVia(WriteInvoker)
        .Run();
}

TError TSession::DoAbortWriter()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error.IsOK()) {
        return Error;
    }

    LOG_DEBUG("Started aborting chunk writer");

    PROFILE_TIMING ("/chunk_io/chunk_abort_time") {
        try {
            Writer->Abort();
        } catch (const std::exception& ex) {
            OnIOError(TError(
                TDataNodeServiceProxy::EErrorCode::IOError,
                "Error aborting chunk: %s",
                ~ToString(ChunkId))
                << ex);
        }
        Writer.Reset();
    }

    LOG_DEBUG("Finished aborting chunk writer");

    return Error;
}

TError TSession::OnWriterAborted(TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseSpaceOccupiedByBlocks();
    return error;
}

TAsyncError TSession::CloseFile(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return
        BIND(&TSession::DoCloseFile,MakeStrong(this), chunkMeta)
        .AsyncVia(WriteInvoker)
        .Run();
}

TError TSession::DoCloseFile(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error.IsOK()) {
        return Error;
    }

    LOG_DEBUG("Started closing chunk writer");

    PROFILE_TIMING ("/chunk_io/chunk_writer_close_time") {
        try {
            Sync(~Writer, &TFileWriter::AsyncClose, chunkMeta);
        } catch (const std::exception& ex) {
            OnIOError(TError(
                TDataNodeServiceProxy::EErrorCode::IOError,
                "Error closing chunk: %s",
                ~ToString(ChunkId))
                << ex);
        }
    }

    LOG_DEBUG("Finished closing chunk writer");

    return Error;
}

TValueOrError<TChunkPtr> TSession::OnFileClosed(TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!error.IsOK()) {
        return error;
    }

    ReleaseSpaceOccupiedByBlocks();
    auto chunk = New<TStoredChunk>(
        Location, 
        ChunkId, 
        Writer->GetChunkMeta(), 
        Writer->GetChunkInfo(),
        Bootstrap->GetMemoryUsageTracker());
    Bootstrap->GetChunkStore()->RegisterChunk(chunk);
    return TChunkPtr(chunk);
}

void TSession::ReleaseBlocks(int flushedBlockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(WindowStartIndex <= flushedBlockIndex);

    while (WindowStartIndex <= flushedBlockIndex) {
        auto& slot = GetSlot(WindowStartIndex);
        YCHECK(slot.State == ESlotState::Written);
        slot.Block = TSharedRef();
        slot.IsWritten.Reset();
        ++WindowStartIndex;
    }

    LOG_DEBUG("Released blocks (WindowStart: %d)",
        WindowStartIndex);
}

bool TSession::IsInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return blockIndex >= WindowStartIndex;
}

void TSession::VerifyInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!IsInWindow(blockIndex)) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::WindowError,
            "Block %d is out of the window (WindowStart: %d, WindowSize: %" PRISZT ")",
            blockIndex,
            WindowStartIndex,
            Window.size());
    }
}

TSession::TSlot& TSession::GetSlot(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(IsInWindow(blockIndex));
    
    Window.reserve(blockIndex + 1);

    while (Window.size() <= blockIndex) {
        // NB: do not use resize here! 
        // Newly added slots must get a fresh copy of IsWritten promise.
        // Using resize would cause all of these slots to share a single promise.
        Window.push_back(TSlot());
    }

    return Window[blockIndex];
}

TSharedRef TSession::GetBlock(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    VerifyInWindow(blockIndex);

    RenewLease();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::WindowError,
            "Trying to retrieve a block %d that is not received yet (WindowStart: %d)",
            blockIndex,
            WindowStartIndex);
    }

    LOG_DEBUG("Chunk block %d retrieved", blockIndex);

    return slot.Block;
}

void TSession::MarkAllSlotsWritten()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Mark all slots as written to notify all guys waiting on Flush.
    FOREACH (auto& slot, Window) {
        if (!slot.IsWritten.IsSet()) {
            slot.IsWritten.Set();
        }
    }
}

void TSession::ReleaseSpaceOccupiedByBlocks()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Location->UpdateUsedSpace(-Size);
}

void TSession::OnIOError(const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error.IsOK())
        return;

    Error = error;

    LOG_ERROR(Error, "Session failed");

    Bootstrap->GetControlInvoker()->Invoke(BIND(
        &TSession::MarkAllSlotsWritten,
        MakeStrong(this)));

    Location->Disable();
}

////////////////////////////////////////////////////////////////////////////////

TSessionManager::TSessionManager(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , SessionCount(0)
{
    YCHECK(config);
    YCHECK(bootstrap);
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

TSessionPtr TSessionManager::StartSession(const TChunkId& chunkId)
{
    auto location = Bootstrap->GetChunkStore()->GetNewChunkLocation();

    auto session = New<TSession>(Config, Bootstrap, chunkId, location);
    session->Start();

    auto lease = TLeaseManager::CreateLease(
        Config->SessionTimeout,
        BIND(
            &TSessionManager::OnLeaseExpired,
            MakeStrong(this),
            session)
        .Via(Bootstrap->GetControlInvoker()));
    session->SetLease(lease);

    AtomicIncrement(SessionCount);
    YCHECK(SessionMap.insert(MakePair(chunkId, session)).second);

    return session;
}

void TSessionManager::CancelSession(TSessionPtr session, const TError& error)
{
    auto chunkId = session->GetChunkId();

    YCHECK(SessionMap.erase(chunkId) == 1);
    AtomicDecrement(SessionCount);

    session->Cancel(error);

    LOG_INFO(error, "Session canceled (ChunkId: %s)",
        ~chunkId.ToString());
}

TFuture< TValueOrError<TChunkPtr> > TSessionManager::FinishSession(
    TSessionPtr session, 
    const TChunkMeta& chunkMeta)
{
    return session
        ->Finish(chunkMeta)
        .Apply(BIND(
            &TSessionManager::OnSessionFinished,
            MakeStrong(this),
            session));
}

TValueOrError<TChunkPtr> TSessionManager::OnSessionFinished(TSessionPtr session, TValueOrError<TChunkPtr> chunkOrError)
{
    YCHECK(SessionMap.erase(session->GetChunkId()) == 1);
    AtomicDecrement(SessionCount);
    LOG_INFO("Session finished (ChunkId: %s)", ~session->GetChunkId().ToString());
    return chunkOrError;
}

void TSessionManager::OnLeaseExpired(TSessionPtr session)
{
    if (SessionMap.find(session->GetChunkId()) != SessionMap.end()) {
        LOG_INFO("Session %s lease expired", ~session->GetChunkId().ToString());
        CancelSession(session, TError("Session lease expired"));
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
