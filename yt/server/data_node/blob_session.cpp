#include "stdafx.h"
#include "blob_session.h"
#include "session_manager.h"
#include "private.h"
#include "config.h"
#include "location.h"
#include "block_store.h"
#include "blob_chunk.h"
#include "chunk_store.h"

#include <core/misc/fs.h>
#include <core/misc/sync.h>

#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

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

TBlobSession::TBlobSession(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap,
    const TChunkId& chunkId,
    EWriteSessionType type,
    bool syncOnClose,
    TLocationPtr location)
    : TSession(
        config,
        bootstrap,
        chunkId,
        type,
        syncOnClose,
        location)
    , WindowStartIndex(0)
    , WriteIndex(0)
    , Size(0)
    , FileName(Location->GetChunkFileName(ChunkId))
{ }

void TBlobSession::Start(TLeaseManager::TLease lease)
{
    TSession::Start(lease);

    WriteInvoker->Invoke(BIND(&TBlobSession::DoOpenFile, MakeStrong(this)));
}

void TBlobSession::DoOpenFile()
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
                "Error creating blob chunk %s",
                ~ToString(ChunkId))
                << ex);
            return;
        }
    }

    LOG_DEBUG("Finished opening chunk writer");
}

TFuture<TErrorOr<IChunkPtr>> TBlobSession::Finish(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CloseLease();

    for (int blockIndex = WindowStartIndex; blockIndex < Window.size(); ++blockIndex) {
        const auto& slot = GetSlot(blockIndex);
        if (slot.State != ESlotState::Empty) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::WindowError,
                "Attempt to finish a blob session with an unflushed block %d",
                blockIndex)
                << TErrorAttribute("window_start", WindowStartIndex)
                << TErrorAttribute("window_size", Window.size());
        }
    }

    LOG_DEBUG("Finishing session");

    return CloseFile(chunkMeta).Apply(
        BIND(&TBlobSession::OnFileClosed, MakeStrong(this))
            .AsyncVia(Bootstrap->GetControlInvoker()));
}

const TChunkInfo& TBlobSession::GetChunkInfo() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Writer->GetChunkInfo();
}

TAsyncError TBlobSession::PutBlocks(
    int startBlockIndex,
    const std::vector<TSharedRef>& blocks,
    bool enableCaching)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

    auto blockStore = Bootstrap->GetBlockStore();

    int blockIndex = startBlockIndex;
    i64 requestSize = 0;

    for (const auto& block : blocks) {
        TBlockId blockId(ChunkId, blockIndex);
        ValidateBlockIsInWindow(blockIndex);

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
    return throttler->Throttle(requestSize).Apply(BIND([] () {
        return TError();
    }));
}

TAsyncError TBlobSession::SendBlocks(
    int startBlockIndex,
    int blockCount,
    const TNodeDescriptor& target)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Ping();

    TDataNodeServiceProxy proxy(ChannelFactory->CreateChannel(target.Address));
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
    return throttler->Throttle(requestSize).Apply(BIND([=] () {
        return req->Invoke().Apply(BIND([=] (TDataNodeServiceProxy::TRspPutBlocksPtr rsp) {
            return rsp->GetError();
        }));
    }));
}

void TBlobSession::EnqueueWrites()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto sessionManager = Bootstrap->GetSessionManager();
    while (WriteIndex < Window.size()) {
        const auto& slot = GetSlot(WriteIndex);
        YCHECK(slot.State == ESlotState::Received || slot.State == ESlotState::Empty);
        if (slot.State == ESlotState::Empty)
            break;

        sessionManager->UpdatePendingWriteSize(slot.Block.Size());

        BIND(
            &TBlobSession::DoWriteBlock,
            MakeStrong(this),
            slot.Block,
            WriteIndex)
        .AsyncVia(WriteInvoker)
        .Run()
        .Subscribe(
            BIND(&TBlobSession::OnBlockWritten, MakeStrong(this), WriteIndex)
                .Via(Bootstrap->GetControlInvoker()));
        ++WriteIndex;
    }
}

TError TBlobSession::DoWriteBlock(const TSharedRef& block, int blockIndex)
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

void TBlobSession::OnBlockWritten(int blockIndex, TError error)
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

    auto sessionManager = Bootstrap->GetSessionManager();
    sessionManager->UpdatePendingWriteSize(-slot.Block.Size());
}

TAsyncError TBlobSession::FlushBlock(int blockIndex)
{
    // TODO: verify monotonicity of blockIndex
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateBlockIsInWindow(blockIndex);
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
    return slot.IsWritten.ToFuture().Apply(
        BIND(&TBlobSession::OnBlockFlushed, MakeStrong(this), blockIndex));
}

TError TBlobSession::OnBlockFlushed(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseBlocks(blockIndex);
    return Error;
}

void TBlobSession::Cancel(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Canceling session");

    CloseLease();
    AbortWriter()
        .Apply(BIND(&TBlobSession::OnWriterAborted, MakeStrong(this))
            .AsyncVia(Bootstrap->GetControlInvoker()));
}

TAsyncError TBlobSession::AbortWriter()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TBlobSession::DoAbortWriter, MakeStrong(this))
        .AsyncVia(WriteInvoker)
        .Run();
}

TError TBlobSession::DoAbortWriter()
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
                "Error aborting chunk %s",
                ~ToString(ChunkId))
                << ex);
        }
        Writer.Reset();
    }

    LOG_DEBUG("Finished aborting chunk writer");

    return Error;
}

TError TBlobSession::OnWriterAborted(TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO(error, "Session canceled");

    ReleaseSpace();
    Failed_.Fire(error);

    return error;
}

TAsyncError TBlobSession::CloseFile(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TBlobSession::DoCloseFile,MakeStrong(this), chunkMeta)
        .AsyncVia(WriteInvoker)
        .Run();
}

TError TBlobSession::DoCloseFile(const TChunkMeta& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error.IsOK()) {
        return Error;
    }

    LOG_DEBUG("Started closing chunk writer (ChunkSize: %" PRId64 ")",
        Writer->GetDataSize());

    PROFILE_TIMING ("/chunk_writer_close_time") {
        try {
            auto result = Writer->Close(chunkMeta).Get();
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

TErrorOr<IChunkPtr> TBlobSession::OnFileClosed(TError error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ReleaseSpace();

    if (!error.IsOK()) {
        LOG_WARNING(error, "Session has failed to finish");
        Failed_.Fire(error);
        return error;
    }

    auto chunk = New<TStoredBlobChunk>(
        Location,
        ChunkId,
        Writer->GetChunkMeta(),
        Writer->GetChunkInfo(),
        &Bootstrap->GetMemoryUsageTracker());

    LOG_INFO("Session finished");

    Completed_.Fire(chunk);
    return IChunkPtr(chunk);
}

void TBlobSession::ReleaseBlocks(int flushedBlockIndex)
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

bool TBlobSession::IsInWindow(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return blockIndex >= WindowStartIndex;
}

void TBlobSession::ValidateBlockIsInWindow(int blockIndex)
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

TBlobSession::TSlot& TBlobSession::GetSlot(int blockIndex)
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

TSharedRef TBlobSession::GetBlock(int blockIndex)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ValidateBlockIsInWindow(blockIndex);

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

void TBlobSession::MarkAllSlotsWritten()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Mark all slots as written to notify all guys waiting on Flush.
    for (auto& slot : Window) {
        if (slot.State != ESlotState::Written) {
            slot.State = ESlotState::Written;
            slot.IsWritten.Set();
        }
    }
}

void TBlobSession::ReleaseSpace()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Location->UpdateUsedSpace(-Size);
}

void TBlobSession::OnIOError(const TError& error)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (!Error.IsOK())
        return;

    Error = error;

    LOG_ERROR(Error, "Session failed");

    Bootstrap->GetControlInvoker()->Invoke(
        BIND(&TBlobSession::MarkAllSlotsWritten, MakeStrong(this)));

    Location->Disable();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
