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
    using typename TBase::TSession;
    
    using TBase::State;
    using TBase::Logger;
    using TBase::CurrentSession;

    // Protects CompleteReaderCount, ReadySessions, CurrentSession.
    TSpinLock SpinLock;
    std::vector<typename TBase::TSession> ReadySessions;
    std::vector<typename TBase::TSession> CompleteSessions;

    int CompleteReaderCount;

    void OnReaderOpened(
        const typename TBase::TSession& session, 
        TError error) override;

    void OnReaderReady(const typename TBase::TSession& session, TError error);

    void ProcessReadyReader(typename TBase::TSession session);
    void FinishReader(const typename TBase::TSession& session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_PARALLEL_READER_INL_H_
#include "multi_chunk_parallel_reader-inl.h"
#undef MULTI_CHUNK_PARALLEL_READER_INL_H_