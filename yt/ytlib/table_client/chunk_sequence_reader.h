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
        const TReaderOptions& options = TReaderOptions());

    virtual TAsyncError AsyncOpen();

    virtual TAsyncError AsyncNextRow();

    virtual bool IsValid() const;

    virtual TRow& GetRow();
    virtual const TNonOwningKey& GetKey() const;
    virtual const NYTree::TYson& GetRowAttributes() const;

    double GetProgress() const;

    //! Upper estimate. Becomes more and more precise as reading goes.
    i64 GetRowCount() const;

    //! Rough upper estimate.
    i64 GetValueCount() const;

private:
    void PrepareNextChunk();

    void OnReaderOpened(
        TChunkReaderPtr reader, 
        int chunkIndex,
        TError error);

    void SwitchCurrentChunk(TChunkReaderPtr nextReader);
    void OnRowFetched(TError error);

    TChunkSequenceReaderConfigPtr Config;
    NChunkClient::IBlockCachePtr BlockCache;
    std::vector<NProto::TInputChunk> InputChunks;
    TReaderOptions Options;

    //! Upper bound estimation.
    i64 TotalValueCount;

    //! Upper bound, estimation, becomes more precise as we start actually reading chunk.
    //! Is precise and equal to CurrentRowIndex when reading is complete.
    i64 TotalRowCount;

    i64 CurrentRowIndex;

    NRpc::IChannelPtr MasterChannel;
    TAsyncStreamState State;

    int CurrentReader;
    int PartitionTag;

    /*!
     *  If #TReaderOptions::KeepBlocks option is set then the reader keeps references
     *  to all chunk readers it has opened during its lifetime.
     */
    std::vector< TPromise<TChunkReaderPtr> > Readers;
    int LastInitializedReader;
    int LastPreparedReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
