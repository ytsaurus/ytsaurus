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
    typedef TMultiChunkReaderBase<TChunkReader> TBase;

    TMultiChunkParallelReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const typename TBase::TProviderPtr& readerProvider);

    virtual TAsyncError AsyncOpen() override;
    virtual bool FetchNextItem() override;
    virtual bool IsValid() const override;

private:
    using typename TBase::TProviderPtr;
    using typename TBase::TReaderPtr;
    
    using TBase::State;
    using TBase::Logger;
    using TBase::CurrentReader_;

    TSpinLock SpinLock;
    std::vector<typename TBase::TReaderPtr> ReadyReaders;
    std::vector<typename TBase::TReaderPtr> CompleteReaders;

    int CompleteReaderCount;

    void OnReaderOpened(
        const typename TBase::TReaderPtr& chunkReader, 
        int inputChunkIndex, 
        TError error) override;
    void OnReaderReady(const typename TBase::TReaderPtr& chunkReader, TError error);

    void ProcessReadyReader(typename TBase::TReaderPtr chunkReader);
    void FinishReader(const typename TBase::TReaderPtr& chunkReader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_PARALLEL_READER_INL_H_
#include "multi_chunk_parallel_reader-inl.h"
#undef MULTI_CHUNK_PARALLEL_READER_INL_H_