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
    , OnReadyEventCallback_(
        BIND(&TEncodingWriter::OnReadyEvent, MakeWeak(this))
            .Via(CompressionInvoker_))
    , TriggerWritingCallback_(
        BIND(&TEncodingWriter::TriggerWriting, MakeWeak(this))
            .Via(CompressionInvoker_))
{
    Logger = ChunkClientLogger;
    Logger.AddTag("ChunkId: %v", ChunkWriter_->GetChunkId());
}

TEncodingWriter::~TEncodingWriter()
{ }

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
    SetCompressionRatio(double(CompressedSize_.load()) / UncompressedSize_.load());

    if (sizeToRelease > 0) {
        Semaphore_.Release(sizeToRelease);
    } else {
        Semaphore_.Acquire(-sizeToRelease);
    }

    PendingBlocks_.push_back(block);
    LOG_DEBUG("Pending block added (Block: %v)", AddedBlockIndex_);

    ++AddedBlockIndex_;

    if (PendingBlocks_.size() == 1) {
        TriggerWritingCallback_.Run();
    }
}

void TEncodingWriter::OnReadyEvent(const TError& error)
{
    if (!error.IsOK()) {
        State_.Fail(error);
        return;
    }

    YCHECK(IsWaiting_);
    IsWaiting_ = false;

    if (CloseRequested_) {
        State_.FinishOperation();
        return;
    }

    WritePendingBlocks();
}

void TEncodingWriter::TriggerWriting()
{
    if (IsWaiting_) {
        return;
    }

    WritePendingBlocks();
}

// Serialized compression invoker affinity (don't use thread affinity because of thread pool).
void TEncodingWriter::WritePendingBlocks()
{
    while (!PendingBlocks_.empty()) {
        LOG_DEBUG("Writing pending block (Block: %v)", WrittenBlockIndex_);

        auto& front = PendingBlocks_.front();
        auto result = ChunkWriter_->WriteBlock(front);
        Semaphore_.Release(front.Size());
        PendingBlocks_.pop_front();
        ++WrittenBlockIndex_;

        if (!result) {
            IsWaiting_ = true;
            ChunkWriter_->GetReadyEvent().Subscribe(OnReadyEventCallback_);
            break;
        }
    }
}

bool TEncodingWriter::IsReady() const
{
    return Semaphore_.IsReady() && State_.IsActive();
}

TFuture<void> TEncodingWriter::GetReadyEvent()
{
    if (!Semaphore_.IsReady()) {
        State_.StartOperation();

        Semaphore_.GetReadyEvent().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            State_.FinishOperation(error);
        }));
    }

    return State_.GetOperationError();
}

TFuture<void> TEncodingWriter::Flush()
{
    State_.StartOperation();

    Semaphore_.GetFreeEvent().Subscribe(
        BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            if (IsWaiting_) {
                // We dumped all data to ReplicationWriter, and subscribed on ReadyEvent.
                CloseRequested_ = true;
            } else {
                State_.FinishOperation(error);
            }
        }).Via(CompressionInvoker_));

    return State_.GetOperationError();
}

i64 TEncodingWriter::GetUncompressedSize() const
{
    return UncompressedSize_.load();
}

i64 TEncodingWriter::GetCompressedSize() const
{
    // NB: #CompressedSize_ may have not been updated yet (updated in compression invoker).
    return static_cast<i64>(GetUncompressedSize() * GetCompressionRatio());
}

void TEncodingWriter::SetCompressionRatio(double value)
{
    CompressionRatio_ = value;
}

double TEncodingWriter::GetCompressionRatio() const
{
    return CompressionRatio_.load();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
