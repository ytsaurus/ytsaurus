#pragma once

#include "public.h"
#include "private.h"
#include "async_reader.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkReader>
class TMultiChunkReaderBase
    : public virtual TRefCounted
{
    DEFINE_BYREF_RO_PROPERTY(TIntrusivePtr<TChunkReader>, CurrentReader);
    DEFINE_BYVAL_RO_PROPERTY(i64, ItemIndex);
    DEFINE_BYVAL_RO_PROPERTY(i64, ItemCount);
    DEFINE_BYVAL_RO_PROPERTY(bool, IsFetchingComplete);

public:
    typedef TIntrusivePtr<typename TChunkReader::TProvider> TProviderPtr;

    TMultiChunkReaderBase(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks,
        const TProviderPtr& readerProvider);

    virtual TAsyncError AsyncOpen() = 0;
    virtual bool FetchNextItem() = 0;
    TAsyncError GetReadyEvent();

    virtual bool IsValid() const = 0;

protected:
    typedef TIntrusivePtr<TChunkReader> TReaderPtr;

    TTableReaderConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;

    NChunkClient::IBlockCachePtr BlockCache;
    std::vector<NProto::TInputChunk> InputChunks;

    TProviderPtr ReaderProvider;

    TAsyncStreamState State;

    int LastPreparedReader;

    TParallelAwaiterPtr FetchingCompleteAwaiter;

    NLog::TLogger& Logger;
    DECLARE_THREAD_AFFINITY_SLOT(ReaderThread);

    virtual void OnReaderOpened(const TReaderPtr& chunkReader, int chunkIndex, TError error) = 0;

    void PrepareNextChunk();
    void ProcessOpenedReader(const TReaderPtr& reader, int inputChunkIndex);
    void ProcessFinishedReader(const TReaderPtr& reader);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define MULTI_CHUNK_READER_BASE_INL_H_
#include "multi_chunk_reader_base-inl.h"
#undef MULTI_CHUNK_READER_BASE_INL_H_
