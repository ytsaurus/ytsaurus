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
    , CurrentBlockIndex_(0)
    , LargestBlockSize_(0)
{ 
    MiscExt_.set_compression_codec(options->CompressionCodec);
}

void TEncodingChunkWriter::WriteBlock(std::vector<TSharedRef>&& data)
{
    ++CurrentBlockIndex_;

    i64 blockSize = 0;
    for (auto& part : data) {
        blockSize += part.Size();
    }
    LargestBlockSize_ = std::max(LargestBlockSize_, blockSize);

    EncodingWriter_->WriteBlock(std::move(data));
}

TError TEncodingChunkWriter::Close()
{
    auto error = WaitFor(EncodingWriter_->Flush());
    if (!error.IsOK()) {
        return error;
    }

    MiscExt_.set_uncompressed_data_size(EncodingWriter_->GetUncompressedSize());
    MiscExt_.set_compressed_data_size(EncodingWriter_->GetCompressedSize());
    MiscExt_.set_max_block_size(LargestBlockSize_);
    MiscExt_.set_meta_size(Meta_.ByteSize());
    SetProtoExtension(Meta_.mutable_extensions(), MiscExt_);

    return WaitFor(ChunkWriter_->Close(Meta_));
}

TAsyncError TEncodingChunkWriter::GetReadyEvent() const
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
