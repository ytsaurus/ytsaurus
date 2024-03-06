#include "encoding_writer.h"
#include "private.h"
#include "block_cache.h"
#include "chunk_writer.h"
#include "config.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TEncodingWriter::TEncodingWriter(
    TEncodingWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , Options_(std::move(options))
    , ChunkWriter_(std::move(chunkWriter))
    , BlockCache_(std::move(blockCache))
    , Logger(std::move(logger))
    , SizeSemaphore_(New<TAsyncSemaphore>(Config_->EncodeWindowSize))
    , CodecSemaphore_(New<TAsyncSemaphore>(Config_->CompressionConcurrency))
    , Codec_(NCompression::GetCodec(Options_->CompressionCodec))
    , CompressionInvoker_(CreateFixedPriorityInvoker(
        NRpc::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
        Config_->WorkloadDescriptor.GetPriority()))
    , WritePendingBlockCallback_(BIND(
        &TEncodingWriter::WritePendingBlock,
        MakeWeak(this)).Via(CompressionInvoker_))
    , CompressionRatio_(Config_->DefaultCompressionRatio)
    , CodecTime_({Options_->CompressionCodec, TDuration::Zero()})
{ }

void TEncodingWriter::WriteBlock(
    TSharedRef block,
    EBlockType blockType,
    std::optional<int> groupIndex)
{
    block = TrackMemory(Options_->MemoryReferenceTracker, std::move(block));

    EnsureOpen();

    UncompressedSize_ += block.Size();
    SizeSemaphore_->Acquire(block.Size());

    auto blockFuture = CodecSemaphore_->AsyncAcquire().ApplyUnique(
        BIND(
            ThrowOnDestroyed(&TEncodingWriter::DoCompressBlock),
            MakeWeak(this),
            std::move(block),
            blockType,
            AddedBlockIndex_,
            groupIndex)
        .AsyncVia(CompressionInvoker_));
    PendingBlocks_.Enqueue(blockFuture);

    YT_LOG_DEBUG("Pending block added (Block: %v)", AddedBlockIndex_);

    ++AddedBlockIndex_;
}

void TEncodingWriter::WriteBlock(
    std::vector<TSharedRef> vectorizedBlock,
    EBlockType blockType,
    std::optional<int> groupIndex)
{
    for (auto& part : vectorizedBlock) {
        part = TrackMemory(Options_->MemoryReferenceTracker, std::move(part));
    }

    EnsureOpen();

    for (const auto& part : vectorizedBlock) {
        SizeSemaphore_->Acquire(part.Size());
        UncompressedSize_ += part.Size();
    }

    auto blockFuture = CodecSemaphore_->AsyncAcquire().ApplyUnique(
        BIND(
            &TEncodingWriter::DoCompressVector,
            MakeStrong(this),
            std::move(vectorizedBlock),
            blockType,
            AddedBlockIndex_,
            groupIndex)
        .AsyncVia(CompressionInvoker_));
    PendingBlocks_.Enqueue(blockFuture);

    YT_LOG_DEBUG("Pending block added (Block: %v)", AddedBlockIndex_);

    ++AddedBlockIndex_;
}

void TEncodingWriter::EnsureOpen()
{
    if (!OpenFuture_) {
        OpenFuture_ = ChunkWriter_->Open();
        OpenFuture_.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            if (!error.IsOK()) {
                CompletionError_.TrySet(error);
            } else {
                YT_LOG_DEBUG("Underlying session for encoding writer opened (ChunkId: %v)",
                    ChunkWriter_->GetChunkId());
                PendingBlocks_.Dequeue().Subscribe(
                    WritePendingBlockCallback_);
            }
        }));
    }
}

void TEncodingWriter::CacheUncompressedBlock(
    const TSharedRef& block,
    EBlockType blockType,
    int blockIndex)
{
    // We cannot cache blocks before chunk writer is open, since we do not know the #ChunkId.
    auto blockId = TBlockId(ChunkWriter_->GetChunkId(), blockIndex);
    BlockCache_->PutBlock(blockId, blockType, TBlock(block));
}

TBlock TEncodingWriter::DoCompressBlock(
    const TSharedRef& uncompressedBlock,
    EBlockType blockType,
    int blockIndex,
    std::optional<int> groupIndex,
    TAsyncSemaphoreGuard&&)
{
    YT_LOG_DEBUG("Started compressing block (Block: %v, Codec: %v)",
        blockIndex,
        Codec_->GetId());

    TBlock compressedBlock;
    compressedBlock.GroupIndex = groupIndex;
    {
        TFiberWallTimer timer;
        auto finally = Finally([&] {
            auto guard = WriterGuard(CodecTimeLock_);
            CodecTime_.CpuDuration += timer.GetElapsedTime();
        });
        compressedBlock.Data = Codec_->Compress(uncompressedBlock);
    }

    compressedBlock.Data = TrackMemory(Options_->MemoryReferenceTracker, std::move(compressedBlock.Data));

    if (Config_->ComputeChecksum) {
        compressedBlock.Checksum = GetChecksum(compressedBlock.Data);
    }

    if (Config_->VerifyCompression) {
        VerifyBlock(uncompressedBlock, compressedBlock.Data);
    }

    YT_LOG_DEBUG("Finished compressing block (Block: %v, Codec: %v)",
        blockIndex,
        Codec_->GetId());

    if (Any(BlockCache_->GetSupportedBlockTypes() & blockType)) {
        YT_UNUSED_FUTURE(OpenFuture_.Apply(BIND(
            &TEncodingWriter::CacheUncompressedBlock,
            MakeWeak(this),
            uncompressedBlock,
            blockType,
            blockIndex)));
    }

    auto sizeToRelease = -static_cast<i64>(compressedBlock.Size()) + uncompressedBlock.Size();
    ProcessCompressedBlock(sizeToRelease);

    return compressedBlock;
}

TBlock TEncodingWriter::DoCompressVector(
    const std::vector<TSharedRef>& uncompressedVectorizedBlock,
    EBlockType blockType,
    int blockIndex,
    std::optional<int> groupIndex,
    TAsyncSemaphoreGuard&&)
{
    YT_LOG_DEBUG("Started compressing block (Block: %v, Codec: %v)",
        blockIndex,
        Codec_->GetId());

    TBlock compressedBlock;
    compressedBlock.GroupIndex = groupIndex;
    {
        TFiberWallTimer timer;
        auto finally = Finally([&] {
            auto guard = WriterGuard(CodecTimeLock_);
            CodecTime_.CpuDuration += timer.GetElapsedTime();
        });
        compressedBlock.Data = Codec_->Compress(uncompressedVectorizedBlock);
    }

    compressedBlock.Data = TrackMemory(Options_->MemoryReferenceTracker, std::move(compressedBlock.Data));

    if (Config_->ComputeChecksum) {
        compressedBlock.Checksum = GetChecksum(compressedBlock.Data);
    }

    if (Config_->VerifyCompression) {
        VerifyVector(uncompressedVectorizedBlock, compressedBlock.Data);
    }

    YT_LOG_DEBUG("Finished compressing block (Block: %v, Codec: %v)",
        blockIndex,
        Codec_->GetId());

    if (Any(BlockCache_->GetSupportedBlockTypes() & blockType)) {
        struct TMergedTag { };
        // Handle none codec separately to avoid merging block parts twice.
        auto uncompressedBlock = Options_->CompressionCodec == NCompression::ECodec::None
            ? compressedBlock.Data
            : MergeRefsToRef<TMergedTag>(uncompressedVectorizedBlock);
        YT_UNUSED_FUTURE(OpenFuture_.Apply(BIND(
            &TEncodingWriter::CacheUncompressedBlock,
            MakeWeak(this),
            uncompressedBlock,
            blockType,
            blockIndex)));
    }

    auto sizeToRelease = -static_cast<i64>(compressedBlock.Size()) + GetByteSize(uncompressedVectorizedBlock);
    ProcessCompressedBlock(sizeToRelease);

    return compressedBlock;
}

void TEncodingWriter::VerifyVector(
    const std::vector<TSharedRef>& uncompressedVectorizedBlock,
    const TSharedRef& compressedBlock)
{
    auto decompressedBlock = Codec_->Decompress(compressedBlock);

    YT_LOG_FATAL_IF(
        decompressedBlock.Size() != GetByteSize(uncompressedVectorizedBlock),
        "Compression verification failed");

    const char* current = decompressedBlock.Begin();
    for (const auto& block : uncompressedVectorizedBlock) {
        YT_LOG_FATAL_IF(
            !TRef::AreBitwiseEqual(TRef(current, block.Size()), block),
            "Compression verification failed");
        current += block.Size();
    }
}

void TEncodingWriter::VerifyBlock(
    const TSharedRef& uncompressedBlock,
    const TSharedRef& compressedBlock)
{
    auto decompressedBlock = Codec_->Decompress(compressedBlock);
    YT_LOG_FATAL_IF(
        !TRef::AreBitwiseEqual(decompressedBlock, uncompressedBlock),
        "Compression verification failed");
}

void TEncodingWriter::ProcessCompressedBlock(i64 sizeToRelease)
{
    if (sizeToRelease > 0) {
        SizeSemaphore_->Release(sizeToRelease);
    } else {
        SizeSemaphore_->Acquire(-sizeToRelease);
    }
}

void TEncodingWriter::WritePendingBlock(const TErrorOr<TBlock>& blockOrError)
{
    YT_VERIFY(blockOrError.IsOK());

    const auto& block = blockOrError.Value();
    if (!block) {
        // Sentinel element.
        CompletionError_.Set(TError());
        return;
    }

    // NB(psushin): We delay updating compressed size until passing it to underlying invoker,
    // in order not to look suspicious when writing data is much slower than compression, but not completely stalled;
    // otherwise merge jobs on loaded cluster may seem suspicious.
    CompressedSize_ += block.Size();
    CompressionRatio_ = double(CompressedSize_) / UncompressedSize_;

    YT_LOG_DEBUG("Writing pending block (Block: %v)", WrittenBlockIndex_);

    auto isReady = ChunkWriter_->WriteBlock(Config_->WorkloadDescriptor, block);
    ++WrittenBlockIndex_;

    auto finally = Finally([&] (){
        SizeSemaphore_->Release(block.Size());
    });

    if (!isReady) {
        auto error = WaitFor(ChunkWriter_->GetReadyEvent());
        if (!error.IsOK()) {
            CompletionError_.Set(error);
            return;
        }
    }

    PendingBlocks_.Dequeue().Subscribe(
        WritePendingBlockCallback_);
}

bool TEncodingWriter::IsReady() const
{
    return SizeSemaphore_->IsReady() && !CompletionError_.IsSet();
}

TFuture<void> TEncodingWriter::GetReadyEvent()
{
    return AnySet(std::vector{
        CompletionError_.ToFuture().ToUncancelable(),
        SizeSemaphore_->GetReadyEvent().ToUncancelable()});
}

TFuture<void> TEncodingWriter::Flush()
{
    YT_LOG_DEBUG("Flushing encoding writer");

    // This must be the last enqueued element.
    PendingBlocks_.Enqueue(TBlock());
    return CompletionError_.ToFuture();
}

i64 TEncodingWriter::GetUncompressedSize() const
{
    return UncompressedSize_;
}

i64 TEncodingWriter::GetCompressedSize() const
{
    // NB: #CompressedSize_ may have not been updated yet (updated in compression invoker).
    return static_cast<i64>(GetUncompressedSize() * GetCompressionRatio());
}

double TEncodingWriter::GetCompressionRatio() const
{
    return CompressionRatio_.load();
}

TCodecDuration TEncodingWriter::GetCompressionDuration() const
{
    auto guard = ReaderGuard(CodecTimeLock_);
    return CodecTime_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
