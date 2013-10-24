#pragma once

#include "public.h"
#include "schema.h"
#include "config.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk.pb.h>
#include <ytlib/chunk_client/public.h>

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriter
    : public virtual TRefCounted
{
public:
    TChunkWriter(
        TChunkWriterConfigPtr config,
        NChunkClient::TEncodingWriterOptionsPtr options,
        NChunkClient::IAsyncWriterPtr asyncWriter);

    void Open(
        TNameTablePtr nameTable,
        const NProto::TTableSchemaExt& schema,
        const TKeyColumns& keyColumns = TKeyColumns(),
        ERowsetType type = ERowsetType::Simple);

    void WriteValue(const TRowValue& value);
    bool EndRow(TTimestamp timestamp = NullTimestamp, bool deleted = false);

    TAsyncError GetReadyEvent();

    TAsyncError AsyncClose();

    i64 GetRowIndex() const;

private:
    struct TColumnDescriptor
    {
        TColumnDescriptor()
            : IndexInBlock(-1)
            , OutputIndex(-1)
            , Type(EColumnType::Null)
            , IsKeyPart(false)
        { }

        int IndexInBlock;
        int OutputIndex;
        EColumnType Type;
        // Used for versioned rowsets.
        bool IsKeyPart;

        struct {
            TStringBuf String;
            double Double;
            i64 Integer;
        } PreviousValue;
    };

    TChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;
    NChunkClient::IAsyncWriterPtr UnderlyingWriter;

    std::vector<int> KeyIndexes;
    ERowsetType RowsetType;
    TNameTablePtr InputNameTable;
    TNameTablePtr OutputNameTable;

    // Vector is indexed by InputNameTable indexes.
    std::vector<TColumnDescriptor> ColumnDescriptors;

    NChunkClient::TEncodingWriterPtr EncodingWriter;

    std::unique_ptr<TBlockWriter> PreviousBlock;
    std::unique_ptr<TBlockWriter> CurrentBlock;

    bool IsNewKey;

    i64 RowIndex;
    i64 LargestBlockSize;

    // Column sizes for block writer.
    std::vector<int> ColumnSizes;

    NChunkClient::NProto::TChunkMeta Meta;
    NProto::TBlockMetaExt BlockMetaExt;
    NProto::TTableSchemaExt Schema;
    NProto::TIndexExt IndexExt;

    void DoClose(TAsyncErrorPromise result);
    void FlushPreviousBlock();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
