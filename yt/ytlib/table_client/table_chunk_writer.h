#pragma once

#include "public.h"
#include "chunk_writer_base.h"

#include "schema.h"
#include "key.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>
#include <ytlib/misc/codec.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/blob_output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class  TTableChunkWriter
    : public TChunkWriterBase
{
public:
    TTableChunkWriter(
        TChunkWriterConfigPtr config,
        NChunkClient::IAsyncWriterPtr chunkWriter,
        const std::vector<TChannel>& channels,
        const TNullable<TKeyColumns>& keyColumns);

    ~TTableChunkWriter();

    TAsyncError AsyncOpen();

    bool TryWriteRow(TRow& row, const TNonOwningKey& key);

    TAsyncError AsyncClose();

    const TOwningKey& GetLastKey() const;
    const TNullable<TKeyColumns>& GetKeyColumns() const;
    i64 GetRowCount() const;

    i64 GetCurrentSize() const;
    NChunkHolder::NProto::TChunkMeta GetMasterMeta() const;

    i64 GetMetaSize() const;

private:
    friend class TTableChunkSequenceWriter;

    std::vector<TChannel> Channels;
    //! If not null chunk is expected to be sorted.
    TNullable<TKeyColumns> KeyColumns;

    bool IsOpen;

    //! Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TStringBuf, int> ColumnIndexes;

    //! Current size of written data.
    /*!
     *  - This counter is updated every #AsyncEndRow call.
     *  - This is an upper bound approximation of the size of written data.
     *    (Indeed, the counter includes compressed size of complete blocks and
     *    uncompressed size of incomplete blocks.)
     */
    i64 CurrentSize;

    TKey<TBlobOutput> LastKey;

    //! Approximate size of collected samples.
    i64 SamplesSize;

    //! Approximate size of collected index.
    i64 IndexSize;

    std::vector<TChannelWriterPtr> ChannelWriters;

    i64 BasicMetaSize;

    NProto::TSamplesExt SamplesExt;
    //! Only for sorted tables.
    NProto::TBoundaryKeysExt BoundaryKeysExt;
    NProto::TIndexExt IndexExt;

    void PrepareBlock(int channelIndex);

    void OnFinalBlocksWritten();

    void EmitIndexEntry();
    void EmitSample(TRow& row);

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
