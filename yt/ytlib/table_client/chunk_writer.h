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

class  TChunkWriter
    : public IAsyncWriter
{
public:
    typedef TIntrusivePtr<TChunkWriter> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;
        
        i64 BlockSize;
        i64 SamplingSize;
        int CodecId;

        TConfig()
        {
            // Block less than 1Kb is a nonsense.
            Register("block_size", BlockSize)
                .GreaterThan(1024)
                .Default(1024 * 1024);
            Register("sampling_size", SamplingSize)
                .GreaterThanOrEqual(1024)
                .Default(1024);
            Register("codec_id", CodecId)
                .Default(ECodecId::None);
        }
    };

    TChunkWriter(
        TConfig* config, 
        NChunkClient::IAsyncWriter* chunkWriter);

    ~TChunkWriter();

    TAsyncError::TPtr AsyncOpen(
        const NProto::TTableChunkAttributes& attributes);

    TAsyncError::TPtr AsyncEndRow(
        TKey& key,
        std::vector<TChannelWriter::TPtr>& channels);

    TAsyncError::TPtr AsyncClose(
        TKey& lastKey,
        std::vector<TChannelWriter::TPtr>& channels);

    i64 GetCurrentSize() const;
    NChunkServer::TChunkId GetChunkId() const;
    NChunkServer::TChunkYPathProxy::TReqConfirm::TPtr GetConfirmRequest();

private:
    TSharedRef PrepareBlock(
        TChannelWriter::TPtr channel, 
        int channelIndex);

    void AddKeySample(const TKey& key);

private:
    TConfig::TPtr Config;

    ICodec* Codec;

    NChunkClient::IAsyncWriter::TPtr ChunkWriter;

    bool IsOpen;
    bool IsClosed;

    int CurrentBlockIndex;

    i64 LastSampleSize;

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
