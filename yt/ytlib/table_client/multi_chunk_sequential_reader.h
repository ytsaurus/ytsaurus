#pragma once

#include "multi_chunk_reader_base.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TReaderProvider>
class TMultiChunkSequentialReader
    : public TMultiChunkReaderBase<TReaderProvider>
{
public:
    TMultiChunkSequentialReader(
		TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const TReaderProviderPtr& readerProvider);

    virtual TAsyncError AsyncOpen();
    virtual bool FetchNextItem();
    virtual bool IsValid() const;

private:
    

    std::vector< TPromise<TReaderPtr> > Readers;
    int CurrentReaderIndex;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#include "multi_chunk_sequential_reader-inl.h"
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

