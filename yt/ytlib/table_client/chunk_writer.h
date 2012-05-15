#pragma once

#include "public.h"
#include "async_writer.h"

#include "schema.h"
#include "key.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class  TChunkWriter
    : public IAsyncWriter
{
public:
    TChunkWriter(
        TChunkWriterConfigPtr config,
        NChunkClient::IAsyncWriterPtr chunkWriter,
        const std::vector<TChannel>& channels,
        const TNullable<TKeyColumns>& keyColumns);

    ~TChunkWriter();

    virtual TAsyncError AsyncOpen();

    virtual TAsyncError AsyncWriteRow(TRow& row, const TNonOwningKey& key);

    virtual TAsyncError AsyncClose();

    virtual const TOwningKey& GetLastKey() const;
    virtual const TNullable<TKeyColumns>& GetKeyColumns() const;
    virtual i64 GetRowCount() const;

    i64 GetCurrentSize() const;
    NChunkHolder::NProto::TChunkMeta GetMasterMeta() const;

    i64 GetMetaSize() const;

private:
    friend class TChunkSequenceWriter;

    TChunkWriterConfigPtr Config;
    std::vector<TChannel> Channels;
    NChunkClient::IAsyncWriterPtr ChunkWriter;
    //! If not null chunk is expected to be sorted.
    TNullable<TKeyColumns> KeyColumns;

    bool IsOpen;
    bool IsClosed;

    //! Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TStringBuf, int> ColumnIndexes;

    int CurrentBlockIndex;

    //! Total size of completed and sent blocks.
    i64 SentSize;

    //! Current size of written data.
    /*!
     *  - This counter is updated every #AsyncEndRow call.
     *  - This is an upper bound approximation of the size of written data.
     *    (Indeed, the counter includes compressed size of complete blocks and
     *    uncompressed size of incomplete blocks.)
     */
    i64 CurrentSize;

    //! Uncompressed size of completed blocks.
    i64 UncompressedSize;

    //! Approximate data size counting all written rows.
    i64 DataWeight;

    TKey<TBlobOutput> LastKey;

    //! Approximate size of collected samples.
    i64 SamplesSize;

    //! Approximate size of collected index.
    i64 IndexSize;

    double CompressionRatio;

    ICodec* Codec;
    std::vector<TChannelWriterPtr> ChannelWriters;

    i64 BasicMetaSize;

    NChunkHolder::NProto::TChunkMeta Meta;
    NProto::TChannelsExt ChannelsExt;
    NChunkHolder::NProto::TMiscExt MiscExt;
    NProto::TSamplesExt SamplesExt;
    //! Only for sorted tables.
    NProto::TBoundaryKeysExt BoundaryKeysExt;
    NProto::TIndexExt IndexExt;

    TSharedRef PrepareBlock(int channelIndex);

    TAsyncError OnFinalBlocksWritten(TError error);
    
    void EmitIndexEntry();
    void EmitSample(TRow& row);

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
