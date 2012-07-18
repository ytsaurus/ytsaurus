#ifndef CHUNK_SEQUENCE_READER_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_sequence_reader_base.h"
#endif
#undef CHUNK_SEQUENCE_READER_BASE_INL_H_

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TReader>
TChunkSequenceReaderBase<TReader>::TChunkSequenceReaderBase(
    TChunkSequenceReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks)
    : Config(config)
    , BlockCache(blockCache)
    , InputChunks(inputChunks)
    , MasterChannel(masterChannel)
    , CurrentReaderIndex(-1)
    , LastInitializedReader(-1)
    , LastPreparedReader(-1)
{
    LOG_DEBUG("Chunk sequence reader created (ChunkCount: %d)", 
        static_cast<int>(InputChunks.size()));

    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        Readers.push_back(NewPromise<TReaderPtr>());
    }

    for (int i = 0; i < Config->PrefetchWindow; ++i) {
        PrepareNextChunk();
    }
}

template <class TReader>
void TChunkSequenceReaderBase<TReader>::PrepareNextChunk()
{

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
