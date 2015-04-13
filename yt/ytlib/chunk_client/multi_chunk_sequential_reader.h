#pragma once

#include "multi_chunk_reader_base.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
class TMultiChunkSequentialReader
    : public TMultiChunkReaderBase<TChunkReader>
{
public:
    typedef TMultiChunkReaderBase<TChunkReader> TBase;

    TMultiChunkSequentialReader(
        TMultiChunkReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
        typename TBase::TProviderPtr readerProvider);

    virtual TAsyncError AsyncOpen() override;
    virtual bool FetchNext() override;

private:
    using TBase::State;
    using TBase::Logger;
    using TBase::Chunks;
    using TBase::CurrentSession;
    using TBase::ReaderProvider;

    std::vector< TPromise<typename TBase::TSession> > Sessions;
    int CurrentReaderIndex;

    virtual void OnReaderOpened(const typename TBase::TSession& session, TError error) override;
    void SwitchCurrentChunk(typename TBase::TSession nextSession);
    bool ValidateReader();
    void OnItemFetched(TError error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#define MULTI_CHUNK_SEQUENTIAL_READER_INL_H_
#include "multi_chunk_sequential_reader-inl.h"
#undef MULTI_CHUNK_SEQUENTIAL_READER_INL_H_

