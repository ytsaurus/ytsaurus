#include "stdafx.h"
#include "encoding_writer.h"
#include "config.h"
#include "private.h"
#include "dispatcher.h"
#include "chunk_writer.h"
#include "block_cache.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/concurrency/action_queue.h>

#include <core/compression/codec.h>

#include <core/misc/finally.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

///////////////////////////////////////////////////////////////////////////////

TEncodingWriter::TEncodingWriter(
    TEncodingWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache)
    : Config_(config)
    , Options_(options)
    , ChunkWriter_(chunkWriter)
    , BlockCache_(blockCache)
    , CompressionRatio_(config->DefaultCompressionRatio)
    , CompressionInvoker_(CreateSerializedInvoker(TDispatcher::Get()->GetCompressionPoolInvoker()))
    , Semaphore_(Config_->EncodeWindowSize)
    , Codec_(NCompression::GetCodec(options->CompressionCodec))
    , WritePendingBlockCallback_(BIND(
        &TEncodingWriter::WritePendingBlock, 
        MakeWeak(this)))
{
    Logger = ChunkClientLogger;
    Logger.AddTag("ChunkId: %v", ChunkWriter_->GetChunkId());

    PendingBlocks_.Dequeue().Subscribe(WritePendingBlockCallback_);
}

void TEncodingWriter::WriteBlock(TSharedRef block)
{
    UncompressedSize_ += block.Size();
    Semaphore_.Acquire(block.Size());
    CompressionInvoker_->Invoke(BIND(
        &TEncodingWriter::DoCompressBlock,
        MakeStrong(this),
        std::move(block)));
}

void TEncodingWriter::WriteBlock(std::vector<TSharedRef> vectorizedBlock)
{
    for (const auto& part : vectorizedBlock) {
        Semaphore_.Acquire(part.Size());
        UncompressedSize_ += part.Size();
    }
    CompressionInvoker_->Invoke(BIND(
        &TEncodingWriter::DoCompressVector,
        MakeWeak(this),
        std::move(vectorizedBlock)));
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::DoCompressBlock(const TSharedRef& uncompressedBlock)
{
    LOG_DEBUG("Compressing block (Block: %v)", AddedBlockIndex_);

    auto compressedBlock = Codec_->Compress(uncompressedBlock);

    CompressedSize_ += compressedBlock.Size();

    if (Config_->VerifyCompression) {
        VerifyBlock(uncompressedBlock, compressedBlock);
    }

    auto blockId = TBlockId(ChunkWriter_->GetChunkId(), AddedBlockIndex_);
    BlockCache_->Put(blockId, EBlockType::UncompressedData, uncompressedBlock, Null);

    int sizeToRelease = -static_cast<i64>(compressedBlock.Size()) + uncompressedBlock.Size();
    ProcessCompressedBlock(compressedBlock, sizeToRelease);
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::DoCompressVector(const std::vector<TSharedRef>& uncompressedVectorizedBlock)
{
    LOG_DEBUG("Compressing block (Block: %v)", AddedBlockIndex_);

    auto compressedBlock = Codec_->Compress(uncompressedVectorizedBlock);

    CompressedSize_ += compressedBlock.Size();

    if (Config_->VerifyCompression) {
        VerifyVector(uncompressedVectorizedBlock, compressedBlock);
    }

    auto blockId = TBlockId(ChunkWriter_->GetChunkId(), AddedBlockIndex_);
    if (Any(BlockCache_->GetSupportedBlockTypes() & EBlockType::UncompressedData)) {
        // Handle none codec separately to avoid merging block parts twice.
        auto uncompressedBlock = Options_->CompressionCodec == NCompression::ECodec::None
            ? compressedBlock
            : MergeRefs(uncompressedVectorizedBlock);
        BlockCache_->Put(blockId, EBlockType::UncompressedData, uncompressedBlock, Null);
    }

    i64 sizeToRelease = -static_cast<i64>(compressedBlock.Size()) + GetByteSize(uncompressedVectorizedBlock);
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
void TEncodingWriter::ProcessCompressedBlock(const TSharedRef& block, i64 sizeToRelease)
{
    CompressionRatio_ = double(CompressedSize_) / UncompressedSize_;

    if (sizeToRelease > 0) {
        Semaphore_.Release(sizeToRelease);
    } else {
        Semaphore_.Acquire(-sizeToRelease);
    }

    PendingBlocks_.Enqueue(block);
    LOG_DEBUG("Pending block added (Block: %v)", AddedBlockIndex_);

    ++AddedBlockIndex_;
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::WritePendingBlock(const TErrorOr<TSharedRef>& blockOrError)
{
    if (!blockOrError.IsOK()) {
        // Sentinel element.
        CompletionError_.Set(TError());
        return;
    }

    LOG_DEBUG("Writing pending block (Block: %v)", WrittenBlockIndex_);

    auto& block = blockOrError.Value();
    auto isReady = ChunkWriter_->WriteBlock(block);
    ++WrittenBlockIndex_;

    TFinallyGuard finally([&](){
        Semaphore_.Release(block.Size());
    });

    if (!isReady) {
        auto error = WaitFor(ChunkWriter_->GetReadyEvent());
        if (!error.IsOK()) {
            CompletionError_.Set(error);
            return;
        }
    }

    PendingBlocks_.Dequeue().Subscribe(WritePendingBlockCallback_);
}

bool TEncodingWriter::IsReady() const
{
    return Semaphore_.IsReady() && !CompletionError_.IsSet();
}

TFuture<void> TEncodingWriter::GetReadyEvent()
{
    auto promise = NewPromise<void>();
    promise.TrySetFrom(CompletionError_.ToFuture());
    promise.TrySetFrom(Semaphore_.GetReadyEvent());

    return promise.ToFuture();
}

TFuture<void> TEncodingWriter::Flush()
{
    // This must be the last enqueued element.
    BIND([this, this_ = MakeStrong(this)] () {
        PendingBlocks_.Enqueue(TError("Sentinel value"));
    })
    .Via(CompressionInvoker_)
    .Run();
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

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
