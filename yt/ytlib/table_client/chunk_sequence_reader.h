#pragma once

#include "public.h"
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
        NRpc::IChannel::TPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        const std::vector<NProto::TInputChunk>& inputChunks);

    TAsyncError AsyncOpen();

    TAsyncError AsyncNextRow();

    bool IsValid() const;

    TRow& GetRow();
    const NYTree::TYson& GetRowAttributes() const;

    double GetProgress() const;

private:
    void PrepareNextChunk();
    void OnNextReaderOpened(TChunkReaderPtr reader, TError error);
    void SetCurrentChunk(TChunkReaderPtr nextReader);
    void OnNextRow(TError error);

    TChunkSequenceReaderConfigPtr Config;
    NChunkClient::IBlockCachePtr BlockCache;
    std::vector<NProto::TInputChunk> InputChunks;

    NRpc::IChannel::TPtr MasterChannel;

    TAsyncStreamState State;

    int NextChunkIndex;
    TPromise<TChunkReaderPtr> NextReader;
    TChunkReaderPtr CurrentReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
