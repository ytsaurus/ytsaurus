#pragma once

#include "public.h"
#include "schema.h"
#include "config.h"
#include "writer.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <ytlib/chunk_client/chunk.pb.h>
#include <ytlib/chunk_client/public.h>

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): consider moving to cpp
class TChunkWriter
    : public IWriter
{
public:
    TChunkWriter(
        TChunkWriterConfigPtr config,
        NChunkClient::TEncodingWriterOptionsPtr options,
        NChunkClient::IAsyncWriterPtr asyncWriter);

    virtual void Open(
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns = TKeyColumns(),
        ERowsetType type = ERowsetType::Simple) override;

    virtual void WriteValue(const TRowValue& value) override;
    virtual bool EndRow(TTimestamp timestamp = NullTimestamp, bool deleted = false) override;

    virtual TAsyncError GetReadyEvent() override;

    virtual TAsyncError AsyncClose() override;

    virtual i64 GetRowIndex() const override;

private:
    struct TColumnDescriptor
    {
        TColumnDescriptor()
            : IndexInBlock(-1)
            , OutputIndex(-1)
            , Type(EColumnType::Null)
            , IsKeyPart(false)
            , PreviousValue({ 0 })
        { }

        int IndexInBlock;
        int OutputIndex;
        EColumnType Type;
        // Used for versioned rowsets.
        bool IsKeyPart;

        union {
            i64 Integer;
            double Double;
            TStringBuf String;
        } PreviousValue;
    };

    TChunkWriterConfigPtr Config;
    NChunkClient::TEncodingWriterOptionsPtr Options;
    NChunkClient::IAsyncWriterPtr UnderlyingWriter;

    std::vector<int> KeyIds;
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
    TTableSchema Schema;
    NProto::TIndexExt IndexExt;

    void DoClose(TAsyncErrorPromise result);
    void FlushPreviousBlock();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
