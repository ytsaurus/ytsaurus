#pragma once

#include "common.h"

#include "writer.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"

#include "../chunk_client/chunk_writer.h"
#include "../misc/codec.h"
#include "../misc/async_stream_state.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Given a schema and input data creates a sequence of blocks and feeds them to
//! NChunkClient::IWriter.
class  TChunkWriter
    : public IWriter
{
public:
    typedef TIntrusivePtr<TChunkWriter> TPtr;

    struct TConfig
    {
        int BlockSize;

        TConfig()
            // ToDo: make configurable
            : BlockSize(1024 * 1024)
        { }
    };

    TChunkWriter(
        const TConfig& config, 
        IChunkWriter::TPtr chunkWriter, 
        const TSchema& schema,
        ICodec* codec);
    ~TChunkWriter();

    // TODO: -> Open
    TAsyncStreamState::TAsyncResult::TPtr AsyncInit();
    void Write(const TColumn& column, TValue value);

    TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow();

    TAsyncStreamState::TAsyncResult::TPtr AsyncClose();

    void Cancel(const Stroka& errorMessage);

    i64 GetCurrentSize() const;

    const TChunkId& GetChunkId() const;

private:
    TSharedRef PrepareBlock(int channelIndex);
    void ContinueEndRow(
        TAsyncStreamState::TResult result,
        int nextChannel);

    void ContinueClose(
        TAsyncStreamState::TResult result, 
        int channelIndex);
    void FinishClose(TAsyncStreamState::TResult result);
    void OnClosed(TAsyncStreamState::TResult result);

private:
    TAsyncStreamState State;

    TConfig Config;
    IChunkWriter::TPtr ChunkWriter;

    int CurrentBlockIndex;

    TSchema Schema;

    //! Columns already set in current row
    yhash_set<TColumn> UsedColumns;

    yvector<TChannelWriter::TPtr> ChannelWriters;

    ICodec* Codec;

    //! Sum size of completed and sent blocks
    i64 SentSize;

    //! Current size of written data
    i64 CurrentSize;

    NProto::TChunkMeta ChunkMeta;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
