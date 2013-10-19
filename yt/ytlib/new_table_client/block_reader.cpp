#include "stdafx.h"
#include "block_reader.h"

#include <core/yson/varint.h>
#include <util/stream/mem.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TVariableIterator::TVariableIterator(const char* opaque, int count)
    : Opaque(opaque)
    , Count(count)
{
    YASSERT(Count >= 0);
}

bool TVariableIterator::Next(TRowValue* rowValue)
{
    if (!Count) {
        return false;
    }

    ui64 index;
    Opaque += ReadVarUInt64(Opaque, &index);
    rowValue->Index = static_cast<i16>(index);

    ui64 length;
    if (length) {
        Opaque += ReadVarUInt64(Opaque, &length);
        rowValue->Length = static_cast<i32>(length);

        rowValue->Data.Any = Opaque;
        Opaque += length;

        rowValue->Type = EColumnType::Any;
    } else {
        rowValue->Type = EColumnType::Null;
    }

    --Count;
    return true;
}

int TVariableIterator::GetLeftValueCount() const
{
    return Count;
}

////////////////////////////////////////////////////////////////////////////////

TBlockReader::TBlockReader(
    const NProto::TBlockMeta& meta,
    const TSharedRef& block,
    const std::vector<EColumnType>& columnTypes)
    : Meta(meta)
    , Block(block)
    , FixedBuffer(nullptr)
    , VariableBuffer(nullptr)
    , RowIndex(0)
{
    {
        TMemoryInput input(Block.Begin(), Block.Size());

        if (Meta.has_variable_buffer_offset()) {
            VariableColumn = input.Buf();
            VariableBuffer = Block.Begin() + Meta.variable_buffer_offset();
            input.Skip(8 * GetRowCount());
        }

        for (const auto& columnType: columnTypes) {
            TColumn column;
            column.Begin = input.Buf();
            column.Type = columnType;
            Columns.push_back(column);

            int columnWidth =
                column.Type == EColumnType::Integer || column.Type == EColumnType::Double
                ? 8
                : 4;
            input.Skip(columnWidth * GetRowCount());
        }

        for (auto& column: Columns) {
            column.NullBitMap.Load(&input);
        }

        FixedBuffer = input.Buf();
    }

    if (Meta.has_end_of_key_flags_offset()) {
        TMemoryInput input(
            Block.Begin() + Meta.end_of_key_flags_offset(),
            Block.Size() - Meta.end_of_key_flags_offset());
        EndOfKeyFlags.Load(&input);
    }
}

void TBlockReader::JumpTo(int rowIndex)
{
    YCHECK(rowIndex < Meta.row_count());
    RowIndex = rowIndex;
}

void TBlockReader::NextRow()
{
    if (RowIndex < Meta.row_count()) {
        ++RowIndex;
    }
}

bool TBlockReader::EndOfBlock() const
{
    return RowIndex == GetRowCount();
}

int TBlockReader::GetRowCount() const
{
    return Meta.row_count();
}

bool TBlockReader::GetEndOfKeyFlag() const
{
    YASSERT(!EndOfKeyFlags.Empty());
    return EndOfKeyFlags.Get(RowIndex);
}

TRowValue TBlockReader::Read(int index) const
{
    YASSERT(index < Columns.size());
    const auto& column = Columns[index];

    TRowValue value;
    if (column.NullBitMap.Get(RowIndex)) {
        value.Type = column.Type;

        switch (column.Type) {
        case EColumnType::Integer:
            value.Data.Integer = *reinterpret_cast<const i64*>(column.Begin + sizeof(i64) * RowIndex);
            break;
        case EColumnType::Double:
            value.Data.Double = *reinterpret_cast<const double*>(column.Begin + sizeof(double) * RowIndex);
            break;
        case EColumnType::String:
        case EColumnType::Any: {
            ui32 offset = *reinterpret_cast<const ui32*>(column.Begin + sizeof(ui32) * RowIndex);
            ui64 length;
            value.Data.String = FixedBuffer + offset + ReadVarUInt64(FixedBuffer + offset, &length);
            value.Length = static_cast<ui32>(length);
            break;
        }
        default:
            YUNREACHABLE();
        }
    } else {
        value.Type = EColumnType::Null;
    }

    return value;
}

TVariableIterator TBlockReader::GetVariableIterator() const
{
    const char* ptr = VariableColumn + 8 * RowIndex;
    ui32 offset = *reinterpret_cast<const ui32*>(ptr);
    ptr += 4;
    ui32 count = *reinterpret_cast<const ui32*>(ptr);
    return TVariableIterator(VariableBuffer + offset, count);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
