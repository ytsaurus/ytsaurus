#pragma once

#include "common.h"

#include "async_writer.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"
#include <ytlib/table_client/table_chunk_meta.pb.h>

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
                .GreaterThanOrEqual(1000)
                .Default(100000);
            Register("codec_id", CodecId)
                .Default(ECodecId::None);
        }
    };

    TChunkWriter(
        TConfig* config, 
        NChunkClient::IAsyncWriter* chunkWriter);

    ~TChunkWriter();

    TAsyncError AsyncOpen(
        const NProto::TTableChunkAttributes& attributes);

    TAsyncError AsyncEndRow(
        const TKey& key,
        const std::vector<TChannelWriter::TPtr>& channels);

    TAsyncError AsyncClose(
        const std::vector<TChannelWriter::TPtr>& channels);

    i64 GetCurrentSize() const;
    NChunkServer::TChunkId GetChunkId() const;
    NChunkServer::TChunkYPathProxy::TReqConfirm::TPtr GetConfirmRequest();

private:
    TSharedRef PrepareBlock(
        TChannelWriter::TPtr channel, 
        int channelIndex);

    void AddKeySample();

private:
    TConfig::TPtr Config;

    ICodec* Codec;

    NChunkClient::IAsyncWriter::TPtr ChunkWriter;

    bool IsOpen;
    bool IsClosed;

    int CurrentBlockIndex;

    //! Data size written before last sample.
    i64 LastSampleSize;

    //! Total size of completed and sent blocks.
    i64 SentSize;

    //! Current size of written data.
    /*!
     *  1. This counter is updated every #AsyncEndRow call.
     *  2. This is an upper bound approximation of the size of written data, because we take 
     *  into account real size of complete blocks and uncompressed size of the incomplete blocks.
     */
    i64 CurrentSize;

    //! Uncompressed size of completed blocks.
    i64 UncompressedSize;

    TKey LastKey;

    NProto::TTableChunkAttributes Attributes;
    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
