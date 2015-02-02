#include "stdafx.h"

#include "encoding_chunk_writer.h"

#include "chunk_writer.h"
#include "config.h"
#include "encoding_writer.h"

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEncodingChunkWriter::TEncodingChunkWriter(
    TEncodingWriterConfigPtr config,
    TEncodingWriterOptionsPtr options,
    IChunkWriterPtr asyncWriter)
    : ChunkWriter_(asyncWriter)
    , EncodingWriter_(New<TEncodingWriter>(config, options, asyncWriter))
{
    MiscExt_.set_compression_codec(static_cast<int>(options->CompressionCodec));
    MiscExt_.set_eden(options->ChunksEden);
}

void TEncodingChunkWriter::WriteBlock(std::vector<TSharedRef>&& data)
{
    ++CurrentBlockIndex_;

    i64 blockSize = GetTotalSize(data);
    LargestBlockSize_ = std::max(LargestBlockSize_, blockSize);

    EncodingWriter_->WriteBlock(std::move(data));
}

void TEncodingChunkWriter::Close()
{
    WaitFor(EncodingWriter_->Flush())
        .ThrowOnError();

    MiscExt_.set_uncompressed_data_size(EncodingWriter_->GetUncompressedSize());
    MiscExt_.set_compressed_data_size(EncodingWriter_->GetCompressedSize());
    MiscExt_.set_max_block_size(LargestBlockSize_);
    MiscExt_.set_meta_size(Meta_.ByteSize());
    SetProtoExtension(Meta_.mutable_extensions(), MiscExt_);

    WaitFor(ChunkWriter_->Close(Meta_))
        .ThrowOnError();
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

NProto::TDataStatistics TEncodingChunkWriter::GetDataStatistics() const
{
    NProto::TDataStatistics result = NChunkClient::NProto::ZeroDataStatistics();
    if (CurrentBlockIndex_ > 0) {
        result.set_uncompressed_data_size(EncodingWriter_->GetUncompressedSize());
        result.set_compressed_data_size(EncodingWriter_->GetCompressedSize());
        result.set_chunk_count(1);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
