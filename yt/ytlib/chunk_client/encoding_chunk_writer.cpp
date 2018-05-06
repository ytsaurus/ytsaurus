#include "encoding_chunk_writer.h"
#include "chunk_writer.h"
#include "config.h"
#include "encoding_writer.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEncodingChunkWriter::TEncodingChunkWriter(
    TEncodingWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    const NLogging::TLogger& logger)
    : ChunkWriter_(std::move(chunkWriter))
    , EncodingWriter_(New<TEncodingWriter>(
        config,
        options,
        ChunkWriter_,
        blockCache,
        logger))
{
    MiscExt_.set_compression_codec(static_cast<int>(options->CompressionCodec));
    MiscExt_.set_erasure_codec(static_cast<int>(ChunkWriter_->GetErasureCodecId()));
    MiscExt_.set_eden(options->ChunksEden);
}

void TEncodingChunkWriter::WriteBlock(std::vector<TSharedRef> vectorizedBlock)
{
    ++CurrentBlockIndex_;

    i64 blockSize = GetByteSize(vectorizedBlock);
    LargestBlockSize_ = std::max(LargestBlockSize_, blockSize);

    EncodingWriter_->WriteBlock(std::move(vectorizedBlock));
}

void TEncodingChunkWriter::WriteBlock(TSharedRef block)
{
    ++CurrentBlockIndex_;

    LargestBlockSize_ = std::max(LargestBlockSize_, static_cast<i64>(block.Size()));
    EncodingWriter_->WriteBlock(std::move(block));
}

void TEncodingChunkWriter::Close()
{
    WaitFor(EncodingWriter_->Flush())
        .ThrowOnError();

    MiscExt_.set_uncompressed_data_size(EncodingWriter_->GetUncompressedSize());
    MiscExt_.set_compressed_data_size(EncodingWriter_->GetCompressedSize());
    MiscExt_.set_max_block_size(LargestBlockSize_);
    MiscExt_.set_meta_size(Meta_.ByteSize());
    MiscExt_.set_creation_time(TInstant::Now().GetValue());
    SetProtoExtension(Meta_.mutable_extensions(), MiscExt_);

    WaitFor(ChunkWriter_->Close(Meta_))
        .ThrowOnError();

    Closed_ = true;
}

TFuture<void> TEncodingChunkWriter::GetReadyEvent() const
{
    return EncodingWriter_->GetReadyEvent();
}

bool TEncodingChunkWriter::IsReady() const
{
    return EncodingWriter_->IsReady();
}

double TEncodingChunkWriter::GetCompressionRatio() const
{
    return EncodingWriter_->GetCompressionRatio();
}

TChunkId TEncodingChunkWriter::GetChunkId() const
{
    return ChunkWriter_->GetChunkId();
}

NProto::TDataStatistics TEncodingChunkWriter::GetDataStatistics() const
{
    if (Closed_) {
        return ChunkWriter_->GetDataStatistics();
    } else {
        NProto::TDataStatistics result;
        if (CurrentBlockIndex_ > 0) {
            result.set_uncompressed_data_size(EncodingWriter_->GetUncompressedSize());
            result.set_compressed_data_size(EncodingWriter_->GetCompressedSize());
            result.set_chunk_count(1);
        }
        return result;
    }
}

TCodecStatistics TEncodingChunkWriter::GetCompressionStatistics() const
{
    return TCodecStatistics().Append(EncodingWriter_->GetCompressionDuration());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
