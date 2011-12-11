#pragma once

#include "common.h"

#include "writer.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"

#include "../chunk_client/async_writer.h"
#include "../misc/codec.h"
#include "../misc/async_stream_state.h"
#include "../misc/thread_affinity.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Given a schema and input data creates a sequence of blocks 
//! and feeds them to NChunkClient::IAsyncWriter.
class  TChunkWriter
    : public IWriter
{
public:
    typedef TIntrusivePtr<TChunkWriter> TPtr;

    struct TConfig
            : public TConfigBase
    {
        i64 BlockSize;
        int CodecId;

        TConfig()
        {
            // Block less than 1Kb is a nonsense.
            Register("block_size", BlockSize).GreaterThan(1024).Default(1024*1024);
            Register("codec_id", CodecId).Default(ECodecId::None);

            SetDefaults();
        }
    };

    TChunkWriter(
        const TConfig& config, 
        NChunkClient::IAsyncWriter::TPtr chunkWriter, 
        const TSchema& schema);
    ~TChunkWriter();

    TAsyncStreamState::TAsyncResult::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);

    TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow();

    TAsyncStreamState::TAsyncResult::TPtr AsyncClose();

    void Cancel(const Stroka& errorMessage);

    i64 GetCurrentSize() const;

    NChunkClient::TChunkId GetChunkId() const;

private:
    TSharedRef PrepareBlock(int channelIndex);
    void ContinueEndRow(
        TAsyncStreamState::TResult result,
        int nextChannel);

    void ContinueClose(
        TAsyncStreamState::TResult result, 
        int startChannelIndex = 0);
    void OnClosed(TAsyncStreamState::TResult result);

private:
    const TConfig Config;
    const TSchema Schema;

    ICodec* Codec;

    NChunkClient::IAsyncWriter::TPtr ChunkWriter;

    TAsyncStreamState State;

    yvector<TChannelWriter::TPtr> ChannelWriters;

    //! Columns already set in current row
    yhash_set<TColumn> UsedColumns;

    int CurrentBlockIndex;

    //! Sum size of completed and sent blocks
    i64 SentSize;

    //! Current size of written data
    i64 CurrentSize;

    NProto::TTableChunkAttributes Attributes;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
