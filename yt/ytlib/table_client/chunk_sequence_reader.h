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
        NRpc::IChannel* masterChannel,
        NChunkClient::IBlockCache* blockCache,
        const std::vector<NProto::TInputChunk>& inputChunks,
        TChunkReader::TOptions options = TChunkReader::TOptions());

    TAsyncError AsyncOpen();

    TAsyncError AsyncNextRow();

    bool IsValid() const;

    const TRow& GetCurrentRow() const;

    // ToDo: consider removing from here.
    const TKey& GetCurrentKey() const;

private:
    void PrepareNextChunk();
    void OnNextReaderOpened(TChunkReader::TPtr reader, TError error);
    void SetCurrentChunk(TChunkReader::TPtr nextReader);
    void OnNextRow(TError error);

    TConfig::TPtr Config;
    NChunkClient::IBlockCache::TPtr BlockCache;
    std::vector<NProto::TInputChunk> InputChunks;

    TChunkReader::TOptions Options;

    NRpc::IChannelPtr MasterChannel;

    TAsyncStreamState State;

    int NextChunkIndex;
    TPromise<TChunkReader::TPtr> NextReader;
    TChunkReader::TPtr CurrentReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
