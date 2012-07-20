#include "stdafx.h"

#include "partition_chunk_sequence_reader.h"
#include <ytlib/chunk_holder/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkSequenceReader::TPartitionChunkSequenceReader(
    TChunkSequenceReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks)
    : TChunkSequenceReaderBase<TPartitionChunkReader>(config, masterChannel, blockCache, MoveRV(inputChunks), Logger)
    , RowCount_(0)
{
    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        RowCount_ += InputChunks[i].row_count();
    }
}

TPartitionChunkReaderPtr TPartitionChunkSequenceReader::CreateNewReader(
    const NProto::TInputChunk& inputChunk, 
    NChunkClient::IAsyncReaderPtr asyncReader)
{
    auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(inputChunk.extensions());

    return New<TPartitionChunkReader>(
        Config->SequentialReader,
        asyncReader,
        inputChunk.partition_tag(),
        ECodecId(miscExt.codec_id()));
}

bool TPartitionChunkSequenceReader::KeepReaders() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT