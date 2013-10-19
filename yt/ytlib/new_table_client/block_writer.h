#pragma once

#include "public.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/misc/ref.h>
#include <core/misc/chunked_output_stream.h>
#include <core/misc/blob_output.h>

#include <util/generic/bitmap.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TBlockWriter 
{
public:
    struct TBlock
    {
        std::vector<TSharedRef> Data;
        NProto::TBlockMeta Meta;
    };

    TBlockWriter(const std::vector<int> columnSizes);

    void WriteInteger(const TRowValue& value, int index);
    void WriteDouble(const TRowValue& value, int index);
    void WriteString(const TRowValue& value, int index);
    void WriteAny(const TRowValue& value, int index);

    // Stores string in a continious memory region.
    // Return TStingBuf containing stored string.
    TStringBuf WriteKeyString(const TRowValue& value, int index);

    void WriteTimestamp(TTimestamp timestamp, bool deleted, int index);

    void WriteVariable(const TRowValue& value, int index);

    void EndRow();

    void PushEndOfKey(bool endOfKey);

    i64 GetSize() const;
    i64 GetCapacity() const;
    i64 GetRowCount() const;

    TBlock FlushBlock();

private:
    struct TColumn {
        TChunkedOutputStream Stream;
        // Bit is set, if corresponding value is not null.
        TDynBitMap NullBitMap;
        int ValueSize;
    };

    TDynBitMap EndOfKeyFlags;

    std::vector<TColumn> FixedColumns;
    TChunkedOutputStream VariableColumn;

    TChunkedOutputStream VariableBuffer;
    TChunkedOutputStream FixedBuffer;

    // In current row.
    ui32 VariableColumnCount;
    ui32 VariableOffset;
    i64 RowCount;

    ui32 RowSize;

    TBlobOutput IntermediateBuffer;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
