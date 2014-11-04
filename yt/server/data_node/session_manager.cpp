#include "stdafx.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "block_store.h"
#include "chunk.h"
#include "chunk_store.h"

#include <core/misc/fs.h>
#include <core/misc/sync.h>

#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/profiling/scoped_timer.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;
static NProfiling::TRateCounter DiskWriteThroughputCounter("/disk_write_throughput_counter");

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    NChunkClient::EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : Config(config)
    , Bootstrap(bootstrap)
    , ChunkId(chunkId)
    , Type(type)
    , SyncOnClose(syncOnClose)
    , Location(location)
    , WindowStartIndex(0)
    , WriteIndex(0)
    , Size(0)
    , WriteInvoker(CreateSerializedInvoker(Location->GetWriteInvoker()))
    , Logger(DataNodeLogger)
    , Profiler(location->Profiler())
{
    YCHECK(bootstrap);
    YCHECK(location);

    Logger.AddTag(Sprintf("LocationId: %s, ChunkId: %s",
        ~Location->GetId(),
        ~ToString(ChunkId)));

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

    LOG_DEBUG("Session started (SessionType: %s)",
        ~Type.ToString());

    WriteInvoker->Invoke(BIND(&TSession::DoOpenFile, MakeStrong(this)));
}

void TSession::DoOpenFile()
{
    LOG_DEBUG("Started opening chunk writer");

    PROFILE_TIMING ("/chunk_writer_open_time") {
        try {
            Writer = New<TFileWriter>(FileName, SyncOnClose);
            Writer->Open();
        }
        catch (const std::exception& ex) {
            OnIOError(TError(
                NChunkClient::EErrorCode::IOError,
                "Error creating chunk %s",
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

void TSession::Ping()
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
    return ChunkId;
}

EWriteSessionType TSession::GetType() const
{
    return Type;
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

TAsyncError TSession::PutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

    auto blockStore = Bootstrap->GetBlockStore();

    int blockIndex = startBlockIndex;
    i64 requestSize = 0;

    FOREACH (const auto& block, blocks) {
        TBlockId blockId(ChunkId, blockIndex);
        VerifyInWindow(blockIndex);

        if (!Location->HasEnoughSpace(block.Size())) {
            return MakeFuture(TError(
                NChunkClient::EErrorCode::OutOfSpace,
                "No enough space left on node"));
        }

        auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            if (TRef::AreBitwiseEqual(slot.Block, block)) {
                LOG_WARNING("Block is already received (Block: %d)", blockIndex);
                continue;
            }

            return MakeFuture(TError(
                NChunkClient::EErrorCode::BlockContentMismatch,
                "Block %d with a different content already received",
                blockIndex)
                << TErrorAttribute("window_start", WindowStartIndex));
        }

        slot.State = ESlotState::Received;
        slot.Block = block;

        if (enableCaching) {
            blockStore->PutBlock(blockId, block, Null);
        }

        Location->UpdateUsedSpace(block.Size());
        Size += block.Size();
        requestSize += block.Size();

        LOG_DEBUG("Chunk block received (Block: %d)", blockIndex);

        ++blockIndex;
    }

    EnqueueWrites();

    auto throttler = Bootstrap->GetInThrottler(Type);
    LOG_DEBUG("Started waiting for input throttler (StartBlockIndex: %d, RequestSize: %" PRISZT ")",
        startBlockIndex,
        requestSize);
    auto this_ = MakeStrong(this);
    return throttler->Throttle(requestSize).Apply(BIND([this, this_, startBlockIndex, requestSize] () {
        LOG_DEBUG("Finished waiting for input throttler (StartBlockIndex: %d, RequestSize: %" PRISZT ")",
            startBlockIndex,
            requestSize);
        return TError();
    }));
}

TAsyncError TSession::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& target)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

    TProxy proxy(ChannelCache.GetChannel(target.GetDefaultAddress()));
    proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

    auto req = proxy.PutBlocks();
    ToProto(req->mutable_chunk_id(), ChunkId);
    req->set_start_block_index(startBlockIndex);

    i64 requestSize = 0;
    for (int blockIndex = startBlockIndex; blockIndex < startBlockIndex + blockCount; ++blockIndex) {
        auto block = GetBlock(blockIndex);
        req->Attachments().push_back(block);
        requestSize += block.Size();
    }

    auto throttler = Bootstrap->GetOutThrottler(Type);
    LOG_DEBUG("Started waiting for input throttler (StartBlockIndex: %d, RequestSize: %" PRISZT ")",
        startBlockIndex,
        requestSize);
    auto this_ = MakeStrong(this);
    return throttler->Throttle(requestSize).Apply(BIND([this, this_, req, startBlockIndex, requestSize] () {
        LOG_DEBUG("Finished waiting for input throttler (StartBlockIndex: %d, RequestSize: %" PRISZT ")",
            startBlockIndex,
            requestSize);
        return DoSendBlocks(req);
    }));
}

TAsyncError TSession::DoSendBlocks(TProxy::TReqPutBlocksPtr req)
{
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

        Bootstrap->GetSessionManager()->UpdatePendingWriteSize(slot.Block.Size());

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

    LOG_DEBUG("Started writing block %d (BlockSize: %" PRISZT ")",
        blockIndex,
        block.Size());

    TScopedTimer timer;
    try {
        if (!Writer->WriteBlock(block)) {
            auto result = Writer->GetReadyEvent().Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
            YUNREACHABLE();
        }
    } catch (const std::exception& ex) {
        TBlockId blockId(ChunkId, blockIndex);
        OnIOError(TError(
            NChunkClient::EErrorCode::IOError,
            "Error writing chunk block %s",
            ~ToString(blockId))
            << ex);
    }

    auto writeTime = timer.GetElapsed();

    LOG_DEBUG("Finished writing block %d", blockIndex);

    auto& locationProfiler = Location->Profiler();
    locationProfiler.Enqueue("/block_write_size", block.Size());
    locationProfiler.Enqueue("/block_write_time", writeTime.MicroSeconds());
    locationProfiler.Enqueue("/block_write_speed", block.Size() * 1000000 / (1 + writeTime.MicroSeconds()));

    DataNodeProfiler.Increment(DiskWriteThroughputCounter, block.Size());

    return Error;
}

void TSession::OnBlockWritten(int blockIndex, TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    UNUSED(error);

    if (!Error.IsOK()) {
        return;
    }

    auto& slot = GetSlot(blockIndex);
    YCHECK(slot.State == ESlotState::Received);
    slot.State = ESlotState::Written;
    slot.IsWritten.Set();
    Bootstrap->GetSessionManager()->UpdatePendingWriteSize(-slot.Block.Size());
}

TAsyncError TSession::FlushBlock(int blockIndex)
{
    // TODO: verify monotonicity of blockIndex
    VERIFY_THREAD_AFFINITY(ControlThread);

    VerifyInWindow(blockIndex);
    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
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

TFuture< TErrorOr<TChunkPtr> > TSession::Finish(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CloseLease();

    for (int blockIndex = WindowStartIndex; blockIndex < Window.size(); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::WindowError,
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

    PROFILE_TIMING ("/chunk_abort_time") {
        try {
            Writer->Abort();
        } catch (const std::exception& ex) {
            OnIOError(TError(
                NChunkClient::EErrorCode::IOError,
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

    LOG_DEBUG("Started closing chunk writer (ChunkSize: %" PRId64 ")", Writer->GetDataSize());

    PROFILE_TIMING ("/chunk_writer_close_time") {
        try {
            auto result = Writer->AsyncClose(chunkMeta).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        } catch (const std::exception& ex) {
            OnIOError(TError(
                NChunkClient::EErrorCode::IOError,
                "Error closing chunk %s",
                ~ToString(ChunkId))
                << ex);
        }
    }

    LOG_DEBUG("Finished closing chunk writer");

    return Error;
}

TErrorOr<TChunkPtr> TSession::OnFileClosed(TError error)
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
    Bootstrap->GetChunkStore()->RegisterNewChunk(chunk);
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
            NChunkClient::EErrorCode::WindowError,
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

    Ping();

    const auto& slot = GetSlot(blockIndex);
    if (slot.State == ESlotState::Empty) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::WindowError,
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
        if (slot.State != ESlotState::Written) {
            slot.State = ESlotState::Written;
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
    , SessionCounts(EWriteSessionType::GetDomainSize())
    , PendingWriteSize(0)
{
    YCHECK(config);
    YCHECK(bootstrap);
    VERIFY_INVOKER_AFFINITY(Bootstrap->GetControlInvoker(), ControlThread);
}

TSessionPtr TSessionManager::FindSession(const TChunkId& chunkId) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto it = SessionMap.find(chunkId);
    if (it == SessionMap.end())
        return nullptr;

    auto session = it->second;
    session->Ping();
    return session;
}

TSessionPtr TSessionManager::StartSession(
    const TChunkId& chunkId,
    EWriteSessionType type,
    bool syncOnClose)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (static_cast<int>(SessionMap.size()) >= Config->MaxWriteSessions) {
        TError error("Maximum concurrent write session limit %d has been reached",
            Config->MaxWriteSessions);
        LOG_ERROR(error);
        THROW_ERROR(error);
    }
    
    auto chunkStore = Bootstrap->GetChunkStore();
    auto location = chunkStore->GetNewChunkLocation();

    auto session = New<TSession>(
        Config,
        Bootstrap,
        chunkId,
        type,
        syncOnClose,
        location);
    session->Start();

    RegisterSession(session);

    auto lease = TLeaseManager::CreateLease(
        Config->SessionTimeout,
        BIND(
            &TSessionManager::OnLeaseExpired,
            MakeStrong(this),
            session)
        .Via(Bootstrap->GetControlInvoker()));
    session->SetLease(lease);

    return session;
}

void TSessionManager::CancelSession(TSessionPtr session, const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UnregisterSession(session);

    session->Cancel(error);

    LOG_INFO(error, "Session canceled (ChunkId: %s)",
        ~ToString(session->GetChunkId()));
}

TFuture< TErrorOr<TChunkPtr> > TSessionManager::FinishSession(
    TSessionPtr session,
    const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return session->Finish(chunkMeta).Apply(
        BIND(
            &TSessionManager::OnSessionFinished,
            MakeStrong(this),
            session)
        .AsyncVia(Bootstrap->GetControlInvoker()));
}

TErrorOr<TChunkPtr> TSessionManager::OnSessionFinished(TSessionPtr session, TErrorOr<TChunkPtr> chunkOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UnregisterSession(session);

    LOG_INFO("Session finished (ChunkId: %s)",
        ~ToString(session->GetChunkId()));

    return chunkOrError;
}

void TSessionManager::OnLeaseExpired(TSessionPtr session)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (SessionMap.find(session->GetChunkId()) == SessionMap.end())
        return;

    LOG_INFO("Session lease expired (ChunkId: %s)",
        ~ToString(session->GetChunkId()));

    CancelSession(session, TError("Session lease expired"));
}

int TSessionManager::GetSessionCount(EWriteSessionType type) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return SessionCounts[static_cast<int>(type)];
}

i64 TSessionManager::GetPendingWriteSize() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return static_cast<i64>(PendingWriteSize);
}

void TSessionManager::RegisterSession(TSessionPtr session)
{
    ++SessionCounts[session->GetType()];
    YCHECK(SessionMap.insert(std::make_pair(session->GetChunkId(), session)).second);
}

void TSessionManager::UnregisterSession( TSessionPtr session )
{
    --SessionCounts[session->GetType()];
    YCHECK(SessionMap.erase(session->GetChunkId()) == 1);
}

void TSessionManager::UpdatePendingWriteSize(i64 delta)
{
    VERIFY_THREAD_AFFINITY_ANY();

    AtomicIncrement(PendingWriteSize, delta);
}

std::vector<TSessionPtr> TSessionManager::GetSessions() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TSessionPtr> result;
    FOREACH (const auto& pair, SessionMap) {
        result.push_back(pair.second);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
