#pragma once

#include "common.h"
#include "reader.h"
#include "chunk_reader.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/misc/async_stream_state.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSequenceReader
    : public IAsyncReader
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
        const yvector<NChunkServer::TChunkId>& chunkIds,
        // TODO(babenko): fixme, make i64
        int startRow,
        int endRow);

    TAsyncError::TPtr AsyncOpen();

    bool HasNextRow() const;

    TAsyncError::TPtr AsyncNextRow();

    bool NextColumn();

    TValue GetValue() const;

    TColumn GetColumn() const;

    void Cancel(const TError& error);

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
    yvector<NChunkServer::TChunkId> ChunkIds;
    int StartRow;
    int EndRow;

    NRpc::IChannel::TPtr MasterChannel;

    TAsyncStreamState State;

    int NextChunkIndex;
    TFuture<TChunkReader::TPtr>::TPtr NextReader;
    TChunkReader::TPtr CurrentReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
