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

class TChunkSequenceReader
    : public IAsyncReader
{
public:
    TChunkSequenceReader(
        TChunkSequenceReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        const std::vector<NProto::TInputChunk>& inputChunks,
        int partitionTag = DefaultPartitionTag,
        TReaderOptions options = TReaderOptions());

    virtual TAsyncError AsyncOpen();

    virtual TAsyncError AsyncNextRow();

    virtual bool IsValid() const;

    virtual TRow& GetRow();
    virtual const TKey<TFakeStrbufStore>& GetKey() const;
    virtual const NYTree::TYson& GetRowAttributes() const;

    double GetProgress() const;

private:
    void PrepareNextChunk();
    void OnNextReaderOpened(TChunkReaderPtr reader, TError error);
    void SetCurrentChunk(TChunkReaderPtr nextReader);
    void OnRowFetched(TError error);

    TChunkSequenceReaderConfigPtr Config;
    NChunkClient::IBlockCachePtr BlockCache;
    std::vector<NProto::TInputChunk> InputChunks;
    TReaderOptions Options;

    NRpc::IChannelPtr MasterChannel;
    TAsyncStreamState State;

    int NextChunkIndex;
    int PartitionTag;
    TPromise<TChunkReaderPtr> NextReader;

    /*!
     *  References to chunk readers are stored here till destruction if 
     *  KeepBlocks option is set.
     *  Current reader is always Readers.back().
     */
    std::vector<TChunkReaderPtr> Readers;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
