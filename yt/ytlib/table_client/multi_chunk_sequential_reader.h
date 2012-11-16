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
    typedef TMultiChunkReaderBase<TChunkReader> TBase;

    TMultiChunkSequentialReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const typename TBase::TProviderPtr& readerProvider);

    virtual TAsyncError AsyncOpen();
    virtual bool FetchNextItem();
    virtual bool IsValid() const;

private:
    using TBase::State;
    using TBase::Logger;
    using TBase::CurrentSession;

    std::vector< TPromise<typename TBase::TSession> > Sessions;
    int CurrentReaderIndex;

    virtual void OnReaderOpened(const typename TBase::TSession& session, TError error) override;
    void SwitchCurrentChunk(typename TBase::TSession nextSession);
    bool ValidateReader();
    void OnItemFetched(TError error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#include "multi_chunk_sequential_reader-inl.h"
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

