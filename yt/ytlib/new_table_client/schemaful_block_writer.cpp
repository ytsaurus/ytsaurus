#include "stdafx.h"
#include "schemaful_block_writer.h"
#include "unversioned_row.h"

#include <core/misc/varint.h>

#include <core/yson/writer.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const ui32 ZeroOffset = 0;
static const i64 ZeroInteger = 0;
static const double ZeroDouble = 0;

////////////////////////////////////////////////////////////////////////////////

TBlockWriter::TBlockWriter(const std::vector<int> columnSizes)
    : VariableColumnCount(0)
    , VariableOffset(0)
    , RowCount(0)
    , RowSize(0)
{
    for (auto size: columnSizes) {
        TColumn column;
        column.ValueSize = size;
        FixedColumns.push_back(column);
        RowSize += size;
    }
}

void TBlockWriter::WriteTimestamp(TTimestamp value, bool deleted, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 8);
    column.NullBitmap.Push(!deleted);
    column.Stream.DoWrite(&value, sizeof(TTimestamp));
}

void TBlockWriter::WriteInt64(const TUnversionedValue& value, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 8);
    if (value.Type == EValueType::Null) {
        column.NullBitmap.Push(false);
        column.Stream.DoWrite(&ZeroInteger, sizeof(i64));
    } else {
        column.NullBitmap.Push(true);
        column.Stream.DoWrite(&value.Data.Int64, sizeof(i64));
    }
}

void TBlockWriter::WriteUint64(const TUnversionedValue& value, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 8);
    if (value.Type == EValueType::Null) {
        column.NullBitmap.Push(false);
        column.Stream.DoWrite(&ZeroInteger, sizeof(ui64));
    } else {
        column.NullBitmap.Push(true);
        column.Stream.DoWrite(&value.Data.Uint64, sizeof(ui64));
    }
}

void TBlockWriter::WriteDouble(const TUnversionedValue& value, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 8);
    if (value.Type == EValueType::Null) {
        column.NullBitmap.Push(false);
        column.Stream.DoWrite(&ZeroDouble, sizeof(double));
    } else {
        column.NullBitmap.Push(true);
        column.Stream.DoWrite(&value.Data.Double, sizeof(double));
    }
}

void TBlockWriter::WriteBoolean(const TUnversionedValue& value, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 8);
    if (value.Type == EValueType::Null) {
        column.NullBitmap.Push(false);
        column.Stream.DoWrite("\x00", 1);
    } else {
        column.NullBitmap.Push(true);
        column.Stream.DoWrite(value.Data.Boolean ? "\x01" : "\x00", 1);
    }
}

void TBlockWriter::WriteString(const TUnversionedValue& value, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 4);
    if (value.Type == EValueType::Null) {
        column.Stream.DoWrite(&ZeroOffset, sizeof(ui32));
        column.NullBitmap.Push(false);
    } else {
        ui32 offset = FixedBuffer.GetSize();
        FixedBuffer.Advance(WriteVarUint64(FixedBuffer.Preallocate(MaxVarInt64Size), value.Length));
        FixedBuffer.DoWrite(value.Data.String, value.Length);

        column.Stream.DoWrite(&offset, sizeof(ui32));
        column.NullBitmap.Push(true);
    }
}

void TBlockWriter::WriteAny(const TUnversionedValue& value, int index)
{
    WriteString(value, index);
}

TStringBuf TBlockWriter::WriteKeyString(const TUnversionedValue& value, int index)
{
    YASSERT(index < FixedColumns.size());
    auto& column = FixedColumns[index];
    YASSERT(column.ValueSize == 4);
    if (value.Type == EValueType::Null) {
        column.NullBitmap.Push(false);
        column.Stream.DoWrite(&ZeroOffset, sizeof(ui32));
        return TStringBuf(static_cast<char*>(nullptr), static_cast<size_t>(0));
    } else {
        column.NullBitmap.Push(true);
        ui32 offset = FixedBuffer.GetSize();
        column.Stream.DoWrite(&offset, sizeof(ui32));

        FixedBuffer.Advance(WriteVarUint64(FixedBuffer.Preallocate(MaxVarInt64Size), value.Length));
        char* pos = FixedBuffer.Preallocate(value.Length);
        std::copy(value.Data.String, value.Data.String + value.Length, pos);
        FixedBuffer.Advance(value.Length);
        return TStringBuf(pos, value.Length);
    }
}

void TBlockWriter::WriteVariable(const TUnversionedValue& value, int nameTableIndex)
{
    ++VariableColumnCount;

    // Index in name table.
    VariableBuffer.Advance(WriteVarUint64(VariableBuffer.Preallocate(MaxVarInt64Size), nameTableIndex));

    if (value.Type == EValueType::Null) {
       VariableBuffer.Advance(WriteVarUint64(VariableBuffer.Preallocate(MaxVarInt64Size), 0));
    } else if (value.Type == EValueType::Any) {
        // Length
        VariableBuffer.Advance(WriteVarUint64(VariableBuffer.Preallocate(MaxVarInt64Size), value.Length));
        // Yson
        VariableBuffer.DoWrite(value.Data.String, value.Length);
    } else {
        IntermediateBuffer.Clear();
        TYsonWriter writer(&IntermediateBuffer);

        switch (value.Type) {
            case EValueType::Int64:
                writer.OnInt64Scalar(value.Data.Int64);
                break;
            case EValueType::Uint64:
                writer.OnUint64Scalar(value.Data.Uint64);
                break;
            case EValueType::Double:
                writer.OnDoubleScalar(value.Data.Double);
                break;
            case EValueType::Boolean:
                writer.OnDoubleScalar(value.Data.Boolean);
                break;
            case EValueType::String:
                writer.OnStringScalar(TStringBuf(value.Data.String, value.Length));
                break;
            default:
                YUNREACHABLE();
        }

        // Length
        VariableBuffer.Advance(WriteVarUint64(
            VariableBuffer.Preallocate(MaxVarInt64Size),
            IntermediateBuffer.Size()));
        // Yson
        VariableBuffer.DoWrite(IntermediateBuffer.Begin(), IntermediateBuffer.Size());
    }
}

void TBlockWriter::PushEndOfKey(bool endOfKey)
{
    EndOfKeyFlags.Push(endOfKey);
}

void TBlockWriter::EndRow()
{
    VariableColumn.DoWrite(&VariableOffset, sizeof(ui32));
    VariableColumn.DoWrite(&VariableColumnCount, sizeof(ui32));

    VariableColumnCount = 0;
    VariableOffset = VariableBuffer.GetSize();

    ++RowCount;
}

i64 TBlockWriter::GetSize() const
{
    return RowSize * RowCount + VariableBuffer.GetSize() + FixedBuffer.GetSize();
}

i64 TBlockWriter::GetCapacity() const
{
    i64 result = 0;
    for(auto& column: FixedColumns) {
        result += column.Stream.GetCapacity();
    }

    result += FixedBuffer.GetCapacity();
    result += VariableColumn.GetCapacity();
    result += VariableBuffer.GetCapacity();
    return result;
}

i64 TBlockWriter::GetRowCount() const
{
    return RowCount;
}

auto TBlockWriter::FlushBlock() -> TBlock
{
    TBlock result;
    result.Meta.set_row_count(GetRowCount());

    auto insertBuffer = [&] (const std::vector<TSharedRef>& buffer) {
        result.Data.insert(result.Data.end(), buffer.begin(), buffer.end());
    };

    TChunkedOutputStream bitmaskStream;

    i32 variableBufferOffset = 0;
    if (VariableOffset) {
        YASSERT(VariableColumn.GetSize() == GetRowCount() * 8);
        variableBufferOffset += VariableColumn.GetSize();
        auto buffer = VariableColumn.Flush();
        result.Data.insert(result.Data.end(), buffer.begin(), buffer.end());
    }

    for (auto& column: FixedColumns) {
        result.Meta.add_fixed_column_sizes(column.ValueSize);

        YASSERT(column.ValueSize * GetRowCount() == column.Stream.GetSize());
        variableBufferOffset += column.Stream.GetSize();

        insertBuffer(column.Stream.Flush());
        column.NullBitmap.Save(&bitmaskStream);
    }

    variableBufferOffset += bitmaskStream.GetSize();
    insertBuffer(bitmaskStream.Flush());

    variableBufferOffset += FixedBuffer.GetSize();
    insertBuffer(FixedBuffer.Flush());

    i32 endOfKeyOffset = variableBufferOffset;
    if (VariableOffset) {
        result.Meta.set_variable_buffer_offset(variableBufferOffset);
        endOfKeyOffset += VariableBuffer.GetSize();
        insertBuffer(VariableBuffer.Flush());
    }

    i32 blockSize = endOfKeyOffset;
    if (!EndOfKeyFlags.Empty()) {
        result.Meta.set_end_of_key_flags_offset(endOfKeyOffset);
        TChunkedOutputStream stream;
        EndOfKeyFlags.Save(&stream);
        blockSize += stream.GetSize();
        insertBuffer(stream.Flush());
    }

    result.Meta.set_block_size(blockSize);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
