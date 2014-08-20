#pragma once

#include "old_multi_chunk_reader_base.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
class TOldMultiChunkParallelReader
    : public TOldMultiChunkReaderBase<TChunkReader>
{
public:
    typedef TOldMultiChunkReaderBase<TChunkReader> TBase;

    TOldMultiChunkParallelReader(
        TMultiChunkReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr compressedBlockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        std::vector<NChunkClient::NProto::TChunkSpec>&& chunkSpecs,
        typename TBase::TProviderPtr readerProvider);

    virtual TAsyncError AsyncOpen() override;
    virtual bool FetchNext() override;

private:
    using TBase::State;
    using TBase::Logger;
    using TBase::ChunkSpecs;
    using TBase::PrefetchWindow;
    using TBase::CurrentSession;
    using TBase::ReaderProvider;

    // Protects CompleteReaderCount, ReadySessions, CurrentSession.
    TSpinLock SpinLock;
    std::vector<typename TBase::TSession> ReadySessions;
    std::vector<typename TBase::TSession> CompleteSessions;

    volatile int CompleteReaderCount;

    virtual void OnReaderOpened(
        const typename TBase::TSession& session,
        TError error) override;

    void OnReaderReady(const typename TBase::TSession& session, TError error);

    void ProcessReadyReader(typename TBase::TSession session);
    void FinishReader(const typename TBase::TSession& session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#define MULTI_CHUNK_PARALLEL_READER_INL_H_
#include "old_multi_chunk_parallel_reader-inl.h"
#undef MULTI_CHUNK_PARALLEL_READER_INL_H_
