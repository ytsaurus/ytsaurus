#pragma once

#include "multi_chunk_reader_base.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
class TMultiChunkSequentialReader
    : public TMultiChunkReaderBase<TChunkReader>
{
public:
    TMultiChunkSequentialReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const TProviderPtr& readerProvider);

    virtual TAsyncError AsyncOpen() override;
    virtual bool FetchNextItem() override;
    virtual bool IsValid() const override;

private:
    std::vector< TPromise<TReaderPtr> > Readers;
    int CurrentReaderIndex;

    virtual void OnReaderOpened(const TReaderPtr& chunkReader, int chunkIndex, TError error) override;
    void SwitchCurrentChunk(TReaderPtr nextReader);
    bool ValidateReader();
    void OnItemFetched(TError error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#include "multi_chunk_sequential_reader-inl.h"
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

