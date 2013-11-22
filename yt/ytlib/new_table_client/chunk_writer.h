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
        ERowsetType type = ERowsetType::Simple) final override;

    virtual void WriteValue(const TVersionedValue& value) final override;
    virtual void WriteValue(const TUnversionedValue& value) final override;

    virtual bool EndRow() final override;

    virtual TAsyncError GetReadyEvent() final override;

    virtual TAsyncError AsyncClose() final override;

    virtual i64 GetRowIndex() const final override;

private:
    struct TColumnDescriptor
    {
        TColumnDescriptor()
            : IndexInBlock(-1)
            , OutputIndex(-1)
            , Type(EColumnType::Null)
            , IsKeyPart(false)
            , PreviousValue()
        { }

        int IndexInBlock;
        int OutputIndex;
        EColumnType Type;
        // Used for versioned rowsets.
        bool IsKeyPart;

        union {
            i64 Integer;
            double Double;
            struct {
                const char* String;
                size_t Length;
            };
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
