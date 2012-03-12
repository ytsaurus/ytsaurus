#pragma once

#include "common.h"
#include "async_reader.h"
#include "chunk_reader.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/misc/async_stream_state.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceReader
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<TChunkSequenceReader> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NChunkClient::TRemoteReaderConfig::TPtr RemoteReader;
        NChunkClient::TSequentialReader::TConfig::TPtr SequentialReader;

        TConfig()
        {
            Register("remote_reader", RemoteReader).DefaultNew();
            Register("sequential_reader", SequentialReader).DefaultNew();
        }
    };

    TChunkSequenceReader(
        TConfig* config,
        const TChannel& channel,
        const NObjectServer::TTransactionId& transactionId,
        NRpc::IChannel* masterChannel,
        NChunkClient::IBlockCache* blockCache,
        // ToDo: use rvalue reference.
        std::vector<NProto::TChunkSlice>&& chunkSlices);

    TAsyncError::TPtr AsyncOpen();

    TAsyncError::TPtr AsyncNextRow();

    bool IsValid() const;

    const TRow& GetCurrentRow() const;

private:
    void PrepareNextChunk();
    void OnNextReaderOpened(
        TError error, 
        TChunkReader::TPtr reader);
    void SetCurrentChunk(TChunkReader::TPtr nextReader);
    void OnNextRow(TError error);


    TConfig::TPtr Config;
    TChannel Channel;
    NChunkClient::IBlockCache::TPtr BlockCache;
    NObjectServer::TTransactionId TransactionId;
    std::vector<NProto::TChunkSlice> ChunkSlices;

    NRpc::IChannel::TPtr MasterChannel;

    TAsyncStreamState State;

    int NextChunkIndex;
    TFuture<TChunkReader::TPtr>::TPtr NextReader;
    TChunkReader::TPtr CurrentReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
