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
    {
        i64 BlockSize;
        ECodecId CodecId;

        TConfig()
            // ToDo: make configurable
            : BlockSize(1024 * 1024)
            , CodecId(ECodecId::None)
        { }
    };

    TChunkWriter(
        const TConfig& config, 
        NChunkClient::IAsyncWriter::TPtr chunkWriter, 
        const TSchema& schema);
    ~TChunkWriter();

    TAsyncError::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);

    TAsyncError::TPtr AsyncEndRow();

    TAsyncError::TPtr AsyncClose();

    void Cancel(const TError& error);

    i64 GetCurrentSize() const;

    NChunkClient::TChunkId GetChunkId() const;

private:
    TSharedRef PrepareBlock(int channelIndex);
    void ContinueEndRow(
        TError error,
        int nextChannel);

    void ContinueClose(
        TError error, 
        int startChannelIndex = 0);
    void OnClosed(TError error);

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
