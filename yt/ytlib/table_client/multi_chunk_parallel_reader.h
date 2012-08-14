#pragma once

#include "multi_chunk_reader_base.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
class TMultiChunkParallelReader
    : public TMultiChunkReaderBase<TChunkReader>
{
public:
    TMultiChunkParallelReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const TProviderPtr& readerProvider);

    virtual TAsyncError AsyncOpen() override;
    virtual bool FetchNextItem() override;
    virtual bool IsValid() const override;

private:
    TSpinLock SpinLock;
    std::vector<TReaderPtr> ReadyReaders;
    std::vector<TReaderPtr> CompleteReaders;

    int CompleteReaderCount;

    void OnReaderOpened(
        const TReaderPtr& chunkReader, 
        int inputChunkIndex, 
        TError error) override;
    void OnReaderReady(const TReaderPtr& chunkReader, TError error);

    void ProcessReadyReader(TReaderPtr chunkReader);
    void FinishReader(const TReaderPtr& chunkReader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_PARALLEL_READER_INL_H_
#include "multi_chunk_parallel_reader-inl.h"
#undef MULTI_CHUNK_PARALLEL_READER_INL_H_