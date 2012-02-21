#pragma once

#include "common.h"

#include "writer.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"
#include "table_chunk_meta.pb.h"

#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/configurable.h>

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
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;
        
        i64 BlockSize;
        int CodecId;

        TConfig()
        {
            // Block less than 1Kb is a nonsense.
            Register("block_size", BlockSize)
                .GreaterThan(1024)
                .Default(1024 * 1024);
            Register("codec_id", CodecId)
                .Default(ECodecId::None);
        }
    };

    TChunkWriter(
        TConfig* config, 
        NChunkClient::IAsyncWriter* chunkWriter, 
        const TSchema& schema);
    ~TChunkWriter();

    TAsyncError::TPtr AsyncOpen();
    void Write(const TColumn& column, TValue value);

    TAsyncError::TPtr AsyncEndRow();

    TAsyncError::TPtr AsyncClose();

    i64 GetCurrentSize() const;

    NChunkServer::TChunkId GetChunkId() const;

    NChunkServer::TChunkYPathProxy::TReqConfirm::TPtr GetConfirmRequest();

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
    TConfig::TPtr Config;
    const TSchema Schema;

    ICodec* Codec;

    NChunkClient::IAsyncWriter::TPtr ChunkWriter;

    TAsyncStreamState State;

    yvector<TChannelWriter::TPtr> ChannelWriters;

    //! Columns already set in current row.
    yhash_set<TColumn> UsedColumns;

    int CurrentBlockIndex;

    //! Sum size of completed and sent blocks.
    i64 SentSize;

    //! Current size of written data.
    i64 CurrentSize;

    //! Uncompressed size of completed blocks.
    i64 UncompressedSize;

    NProto::TTableChunkAttributes Attributes;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
