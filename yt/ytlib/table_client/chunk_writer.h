#pragma once

#include "public.h"
#include "async_writer.h"

#include "schema.h"
#include "channel_writer.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class  TChunkWriter
    : public IAsyncWriter
{
public:
    TChunkWriter(
        const TChunkWriterConfigPtr& config,
        NChunkClient::IAsyncWriter* chunkWriter,
        const TSchema& schema,
        const TNullable<TKeyColumns>& keyColumns);

    ~TChunkWriter();

    TAsyncError AsyncOpen();

    TAsyncError AsyncWriteRow(const TRow& row, const TKey& key);

    TAsyncError AsyncClose();

    const TNullable<TKeyColumns>& GetKeyColumns() const;
    virtual i64 GetRowCount() const;
    i64 GetCurrentSize() const;
    NChunkServer::TChunkId GetChunkId() const;
    NChunkServer::TChunkYPathProxy::TReqConfirm::TPtr GetConfirmRequest();

private:
    TSharedRef PrepareBlock(
        TChannelWriter::TPtr channel, 
        int channelIndex);

    void AddKeySample();

private:
    const TSchema Schema;
    TChunkWriterConfigPtr Config;

    ICodec* Codec;

    NChunkClient::IAsyncWriter::TPtr ChunkWriter;

    std::vector<TChannelWriter::TPtr> ChannelWriters;

    bool IsOpen;
    bool IsClosed;

    //! Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TStringBuf, int> ColumnIndexes;

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
