#pragma once

#include "public.h"
#include "private.h"

#include <ytlib/chunk_client/input_chunk.pb.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/misc/async_stream_state.h>

#include <ytlib/rpc/public.h>

#include <ytlib/actions/parallel_awaiter.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
class TMultiChunkReaderBase
    : public virtual TRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(i64, ItemIndex);
    DEFINE_BYVAL_RO_PROPERTY(i64, ItemCount);
    DEFINE_BYVAL_RO_PROPERTY(bool, IsFetchingComplete);

public:
    typedef TIntrusivePtr<typename TChunkReader::TProvider> TProviderPtr;

    TMultiChunkReaderBase(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        std::vector<NChunkClient::NProto::TInputChunk>&& inputChunks,
        const TProviderPtr& readerProvider);

    const TIntrusivePtr<TChunkReader>& CurrentReader() const;

    virtual TAsyncError AsyncOpen() = 0;
    virtual bool FetchNextItem() = 0;
    TAsyncError GetReadyEvent();

    std::vector<NChunkClient::TChunkId> GetFailedChunks() const;

    virtual bool IsValid() const = 0;

protected:
    typedef TIntrusivePtr<TChunkReader> TReaderPtr;

    struct TSession
    {
        TReaderPtr Reader;
        int ChunkIndex;

        TSession()
            : ChunkIndex(-1)
        { }

        TSession(TReaderPtr reader, int chunkIndex)
            : Reader(reader)
            , ChunkIndex(chunkIndex)
        { }
    };

    TTableReaderConfigPtr Config;
    int PrefetchWindow;

    NRpc::IChannelPtr MasterChannel;
    NChunkClient::IBlockCachePtr BlockCache;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;

    std::vector<NChunkClient::NProto::TInputChunk> InputChunks;

    TProviderPtr ReaderProvider;

    TSession CurrentSession;

    TAsyncStreamState State;

    // Protects LastPreparedReader;
    TSpinLock NextChunkLock;
    int LastPreparedReader;

    TParallelAwaiterPtr FetchingCompleteAwaiter;

    TSpinLock FailedChunksLock;
    std::vector<NChunkClient::TChunkId> FailedChunks;

    NLog::TLogger& Logger;
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);

    virtual void OnReaderOpened(const TSession& session, TError error) = 0;

    void PrepareNextChunk();
    void ProcessOpenedReader(const TSession& session);
    void ProcessFinishedReader(const  TSession& reader);
    void AddFailedChunk(const TSession& session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_READER_BASE_INL_H_
#include "multi_chunk_reader_base-inl.h"
#undef MULTI_CHUNK_READER_BASE_INL_H_
