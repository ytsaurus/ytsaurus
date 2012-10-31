#pragma once

#include "public.h"
#include "chunk_writer_base.h"
#include "channel_writer.h"

#include "schema.h"
#include "key.h"

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/blob_output.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/codecs/codec.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableChunkWriter
    : public TChunkWriterBase
{
public:
    TTableChunkWriter(
        TChunkWriterConfigPtr config,
        NChunkClient::IAsyncWriterPtr chunkWriter,
        const TChannels& channels,
        const TNullable<TKeyColumns>& keyColumns);

    ~TTableChunkWriter();

    TAsyncError AsyncOpen();

    // Checks column names for uniqueness.
    bool TryWriteRow(const TRow& row);

    // Used internally. All column names are guaranteed to be unique.
    bool TryWriteRowUnsafe(const TRow& row, const TNonOwningKey& key);
    bool TryWriteRowUnsafe(const TRow& row);

    TAsyncError AsyncClose();

    void SetLastKey(const TOwningKey& key);
    const TOwningKey& GetLastKey() const;

    i64 GetRowCount() const;

    i64 GetCurrentSize() const;
    NChunkClient::NProto::TChunkMeta GetMasterMeta() const;
    NChunkClient::NProto::TChunkMeta GetSchedulerMeta() const;

    i64 GetMetaSize() const;

private:
    struct TChannelColumn
    {
        int ColumnIndex;
        TChannelWriterPtr Writer;

        TChannelColumn(const TChannelWriterPtr& channelWriter, int columnIndex) 
            : ColumnIndex(columnIndex)
            , Writer(channelWriter)
        { } 
    };

    struct TColumnInfo {
        i64 LastRow;
        int KeyColumnIndex;
        std::vector<TChannelColumn> Channels;

        TColumnInfo()
            : LastRow(-1)
            , KeyColumnIndex(-1)
        { }
    };

    TChannels Channels;

    bool IsOpen;

    //! Stores mapping from all key columns and channel non-range columns to indexes.
    yhash_map<TStringBuf, TColumnInfo> ColumnMap;
    std::vector<Stroka> ColumnNames;

    // Used for key creation.
    NYTree::TLexer Lexer;

    TNonOwningKey CurrentKey;
    TOwningKey LastKey;

    //! Approximate size of collected samples.
    i64 SamplesSize;

    //! Approximate size of collected index.
    i64 IndexSize;

    i64 BasicMetaSize;

    NProto::TSamplesExt SamplesExt;
    //! Only for sorted tables.
    NProto::TBoundaryKeysExt BoundaryKeysExt;
    NProto::TIndexExt IndexExt;

    void PrepareBlock();

    void OnFinalBlocksWritten(TError error);

    void EmitIndexEntry();
    void EmitSample(const TRow& row);

    void SelectChannels(const TStringBuf& name, TColumnInfo& columnInfo);
    void FinalizeRow(const TRow& row);
    void ProcessKey();
    TColumnInfo& GetColumnInfo(const TStringBuf& name);
    void WriteValue(const std::pair<TStringBuf, TStringBuf>& value, const TColumnInfo& columnInfo);


    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
