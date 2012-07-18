#pragma once

#include "public.h"
#include "private.h"
#include "async_reader.h"

#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/chunk_client/public.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/misc/async_stream_state.h>


namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class TReader>
class TChunkSequenceReaderBase
    : public virtual TRefCounted
{
public:
    TChunkSequenceReaderBase(
        TChunkSequenceReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        std::vector<NProto::TInputChunk>&& inputChunks);

    TAsyncError AsyncOpen();

protected:
    typedef TIntrusivePtr<TReader> TReaderPtr;

    TChunkSequenceReaderConfigPtr Config;
    NChunkClient::IBlockCachePtr BlockCache;
    std::vector<NProto::TInputChunk> InputChunks;

    NRpc::IChannelPtr MasterChannel;
    TAsyncStreamState State;

    int CurrentReaderIndex;
    TReaderPtr CurrentReader;

    /*!
     *  If #TReaderOptions::KeepBlocks option is set then the reader keeps references
     *  to all chunk readers it has opened during its lifetime.
     */
    std::vector< TPromise<TReaderPtr> > Readers;
    int LastInitializedReader;
    int LastPreparedReader;

    virtual TReaderPtr CreateNewReader() = 0;

    void PrepareNextChunk();

    void OnReaderOpened(
        TReaderPtr reader, 
        int chunkIndex,
        TError error);

    void SwitchCurrentChunk(bool dropCurrentReader, TReaderPtr nextReader);
    void OnItemFetched(TError error);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#define CHUNK_SEQUENCE_READER_BASE_INL_H_
#include "chunk_sequence_reader_base-inl.h"
#undef CHUNK_SEQUENCE_READER_BASE_INL_H_
