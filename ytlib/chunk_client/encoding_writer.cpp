#include "encoding_writer.h"
#include "private.h"
#include "block_cache.h"
#include "chunk_writer.h"
#include "config.h"
#include "dispatcher.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/checksum.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEncodingWriter::TEncodingWriter(
    TEncodingWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    const NLogging::TLogger& logger)
    : Config_(std::move(config))
    , Options_(std::move(options))
    , ChunkWriter_(std::move(chunkWriter))
    , BlockCache_(std::move(blockCache))
    , Logger(logger)
    , Semaphore_(New<TAsyncSemaphore>(Config_->EncodeWindowSize))
    , Codec_(NCompression::GetCodec(Options_->CompressionCodec))
    , CompressionInvoker_(CreateSerializedInvoker(CreateFixedPriorityInvoker(
        TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
        Config_->WorkloadDescriptor.GetPriority())))
    , WritePendingBlockCallback_(BIND(
        &TEncodingWriter::WritePendingBlock,
        MakeWeak(this)).Via(CompressionInvoker_))
    , CompressionRatio_(Config_->DefaultCompressionRatio)
    , CodecTime_({Options_->CompressionCodec, TDuration::MicroSeconds(0)})
{ }

void TEncodingWriter::WriteBlock(TSharedRef block)
{
    EnsureOpen();

    UncompressedSize_ += block.Size();
    Semaphore_->Acquire(block.Size());

    CompressionInvoker_->Invoke(BIND(
        &TEncodingWriter::DoCompressBlock,
        MakeWeak(this),
        std::move(block)));
}

void TEncodingWriter::WriteBlock(std::vector<TSharedRef> vectorizedBlock)
{
    EnsureOpen();

    for (const auto& part : vectorizedBlock) {
        Semaphore_->Acquire(part.Size());
        UncompressedSize_ += part.Size();
    }

    CompressionInvoker_->Invoke(BIND(
        &TEncodingWriter::DoCompressVector,
        MakeWeak(this),
        std::move(vectorizedBlock)));
}

void TEncodingWriter::EnsureOpen()
{
    if (!OpenFuture_) {
        OpenFuture_ = ChunkWriter_->Open();
        OpenFuture_.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            if (!error.IsOK()) {
                CompletionError_.TrySet(error);
            } else {
                LOG_DEBUG("Underlying session for encoding writer opened (ChunkId: %v)",
                    ChunkWriter_->GetChunkId());
                PendingBlocks_.Dequeue().Subscribe(
                    WritePendingBlockCallback_);
            }
        }));
    }
}

void TEncodingWriter::CacheUncompressedBlock(const TSharedRef& block, int blockIndex)
{
    // We cannot cache blocks before chunk writer is open, since we do not know the #ChunkId.
    auto blockId = TBlockId(ChunkWriter_->GetChunkId(), blockIndex);
    BlockCache_->Put(blockId, EBlockType::UncompressedData, TBlock(block), Null);
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::DoCompressBlock(const TSharedRef& uncompressedBlock)
{
    LOG_DEBUG("Started compressing block (Block: %v, Codec: %v)",
        AddedBlockIndex_,
        Codec_->GetId());

    TBlock compressedBlock;
    {
        NProfiling::TCpuTimingGuard guard(&CodecTime_.CpuDuration);
        compressedBlock.Data = Codec_->Compress(uncompressedBlock);
    }

    if (Config_->ComputeChecksum) {
        compressedBlock.Checksum = GetChecksum(compressedBlock.Data);
    }

    if (Config_->VerifyCompression) {
        VerifyBlock(uncompressedBlock, compressedBlock.Data);
    }

    LOG_DEBUG("Finished compressing block (Block: %v, Codec: %v)",
        AddedBlockIndex_,
        Codec_->GetId());

    if (Any(BlockCache_->GetSupportedBlockTypes() & EBlockType::UncompressedData)) {
        OpenFuture_.Apply(BIND(
            &TEncodingWriter::CacheUncompressedBlock,
            MakeWeak(this),
            uncompressedBlock,
            AddedBlockIndex_));
    }

    auto sizeToRelease = -static_cast<i64>(compressedBlock.Size()) + uncompressedBlock.Size();
    ProcessCompressedBlock(compressedBlock, sizeToRelease);
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::DoCompressVector(const std::vector<TSharedRef>& uncompressedVectorizedBlock)
{
    LOG_DEBUG("Started compressing block (Block: %v, Codec: %v)",
        AddedBlockIndex_,
        Codec_->GetId());

    TBlock compressedBlock;
    {
        NProfiling::TCpuTimingGuard guard(&CodecTime_.CpuDuration);
        compressedBlock.Data = Codec_->Compress(uncompressedVectorizedBlock);
    }

    if (Config_->ComputeChecksum) {
        compressedBlock.Checksum = GetChecksum(compressedBlock.Data);
    }

    if (Config_->VerifyCompression) {
        VerifyVector(uncompressedVectorizedBlock, compressedBlock.Data);
    }

    LOG_DEBUG("Finished compressing block (Block: %v, Codec: %v)",
        AddedBlockIndex_,
        Codec_->GetId());

    if (Any(BlockCache_->GetSupportedBlockTypes() & EBlockType::UncompressedData)) {
        struct TMergedTag { };
        // Handle none codec separately to avoid merging block parts twice.
        auto uncompressedBlock = Options_->CompressionCodec == NCompression::ECodec::None
            ? compressedBlock.Data
            : MergeRefsToRef<TMergedTag>(uncompressedVectorizedBlock);
        OpenFuture_.Apply(BIND(
            &TEncodingWriter::CacheUncompressedBlock,
            MakeWeak(this),
            uncompressedBlock,
            AddedBlockIndex_));
    }

    auto sizeToRelease = -static_cast<i64>(compressedBlock.Size()) + GetByteSize(uncompressedVectorizedBlock);
    ProcessCompressedBlock(compressedBlock, sizeToRelease);
}

void TEncodingWriter::VerifyVector(
    const std::vector<TSharedRef>& uncompressedVectorizedBlock,
    const TSharedRef& compressedBlock)
{
    auto decompressedBlock = Codec_->Decompress(compressedBlock);

    LOG_FATAL_IF(
        decompressedBlock.Size() != GetByteSize(uncompressedVectorizedBlock),
        "Compression verification failed");

    const char* current = decompressedBlock.Begin();
    for (const auto& block : uncompressedVectorizedBlock) {
        LOG_FATAL_IF(
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
    LOG_FATAL_IF(
        !TRef::AreBitwiseEqual(decompressedBlock, uncompressedBlock),
        "Compression verification failed");
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::ProcessCompressedBlock(const TBlock& block, i64 sizeToRelease)
{
    if (sizeToRelease > 0) {
        Semaphore_->Release(sizeToRelease);
    } else {
        Semaphore_->Acquire(-sizeToRelease);
    }

    PendingBlocks_.Enqueue(block);
    LOG_DEBUG("Pending block added (Block: %v)", AddedBlockIndex_);

    ++AddedBlockIndex_;
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::WritePendingBlock(const TErrorOr<TBlock>& blockOrError)
{
    YCHECK(blockOrError.IsOK());

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

    LOG_DEBUG("Writing pending block (Block: %v)", WrittenBlockIndex_);

    auto isReady = ChunkWriter_->WriteBlock(block);
    ++WrittenBlockIndex_;

    auto finally = Finally([&] (){
        Semaphore_->Release(block.Size());
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
    return Semaphore_->IsReady() && !CompletionError_.IsSet();
}

TFuture<void> TEncodingWriter::GetReadyEvent()
{
    auto promise = NewPromise<void>();
    promise.TrySetFrom(CompletionError_.ToFuture());
    promise.TrySetFrom(Semaphore_->GetReadyEvent());

    return promise.ToFuture();
}

TFuture<void> TEncodingWriter::Flush()
{
    LOG_DEBUG("Flushing encoding writer");

    // This must be the last enqueued element.
    CompressionInvoker_->Invoke(BIND([this, this_ = MakeStrong(this)] () {
        PendingBlocks_.Enqueue(TBlock());
    }));
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

const TCodecDuration& TEncodingWriter::GetCompressionDuration() const
{
    return CodecTime_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
