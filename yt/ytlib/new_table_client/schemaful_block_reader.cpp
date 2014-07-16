#include "stdafx.h"
#include "schemaful_block_reader.h"
#include "unversioned_row.h"

#include <core/misc/varint.h>

#include <util/stream/mem.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

TVariableIterator::TVariableIterator(const char* opaque, int count)
    : Opaque(opaque)
    , Count(count)
{
    YASSERT(Count >= 0);
}

bool TVariableIterator::ParseNext(TUnversionedValue* value)
{
    if (Count == 0) {
        return false;
    }

    ui64 id;
    Opaque += ReadVarUInt64(Opaque, &id);
    YASSERT(id <= std::numeric_limits<ui16>::max());

    value->Id = static_cast<ui16>(id);

    ui64 length;
    Opaque += ReadVarUInt64(Opaque, &length);
    YASSERT(length <= std::numeric_limits<ui32>::max());

    if (length != 0) {
        value->Type = EValueType::Any;
        value->Length = static_cast<ui32>(length);
        value->Data.String = Opaque;
        Opaque += length;
    } else {
        value->Type = EValueType::Null;
    }

    --Count;

    return true;
}

int TVariableIterator::GetRemainingCount() const
{
    return Count;
}

////////////////////////////////////////////////////////////////////////////////

TBlockReader::TBlockReader(
    const NProto::TBlockMeta& meta,
    const TSharedRef& block,
    const std::vector<EValueType>& columnTypes)
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
                column.Type == EValueType::Int64 || column.Type == EValueType::Double
                ? 8
                : 4;
            input.Skip(columnWidth * GetRowCount());
        }

        for (auto& column: Columns) {
            column.NullBitmap.Load(&input);
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

TUnversionedValue TBlockReader::Read(int index) const
{
    YASSERT(index < Columns.size());
    const auto& column = Columns[index];

    TUnversionedValue value;
    if (column.NullBitmap.Get(RowIndex)) {
        value.Type = column.Type;

        switch (column.Type) {
            case EValueType::Int64:
                value.Data.Int64 = *reinterpret_cast<const i64*>(column.Begin + sizeof(i64) * RowIndex);
                break;
            case EValueType::Double:
                value.Data.Double = *reinterpret_cast<const double*>(column.Begin + sizeof(double) * RowIndex);
                break;
            case EValueType::Boolean:
                value.Data.Boolean = *reinterpret_cast<const char*>(column.Begin + 1 * RowIndex) == 1;
                break;
            case EValueType::String:
            case EValueType::Any: {
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
        value.Type = EValueType::Null;
    }

    return value;
}

TVariableIterator TBlockReader::GetVariableIterator() const
{
    if (!Meta.has_variable_buffer_offset()) {
        return TVariableIterator(nullptr, 0);
    } else {
        const char* ptr = VariableColumn + 8 * RowIndex;
        ui32 offset = *reinterpret_cast<const ui32*>(ptr);
        ptr += 4;
        ui32 count = *reinterpret_cast<const ui32*>(ptr);
        return TVariableIterator(VariableBuffer + offset, count);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
