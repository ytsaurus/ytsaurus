#include "schemaless_block_writer.h"
#include "schema.h"

#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/yt/coding/varint.h>

namespace NYT::NTableClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

struct THorizontalSchemalessBlockWriterTag { };

// NB! Must exceed lf_alloc small block size limit.
const i64 THorizontalBlockWriter::MinReserveSize = 64_KB + 1;
const i64 THorizontalBlockWriter::MaxReserveSize = 2_MB;

THorizontalBlockWriter::THorizontalBlockWriter(TTableSchemaPtr schema, i64 reserveSize)
    : ReserveSize_(std::min(
        std::max(MinReserveSize, reserveSize),
        MaxReserveSize))
    , ColumnCount_(schema->GetColumnCount())
    , ColumnHunkFlags_(new bool[ColumnCount_])
    , Offsets_(GetRefCountedTypeCookie<THorizontalSchemalessBlockWriterTag>(), 4_KB, ReserveSize_ / 2)
    , Data_(GetRefCountedTypeCookie<THorizontalSchemalessBlockWriterTag>(), 4_KB, ReserveSize_ / 2)
{
    for (int index = 0; index < ColumnCount_; ++index) {
        const auto& columnSchema = schema->Columns()[index];
        ColumnHunkFlags_[index] = columnSchema.MaxInlineHunkSize().has_value();
    }
}

bool THorizontalBlockWriter::IsInlineHunkValue(const TUnversionedValue& value) const
{
    return value.Id < ColumnCount_ && ColumnHunkFlags_[value.Id] && None(value.Flags & EValueFlags::Hunk);
}

void THorizontalBlockWriter::WriteRow(TUnversionedRow row)
{
    YT_VERIFY(!Closed_);

    ++RowCount_;

    WritePod(Offsets_, static_cast<ui32>(Data_.GetSize()));

    int estimatedSize = MaxVarUint32Size; // value count
    for (auto value : row) {
        estimatedSize += EstimateRowValueSize(value, IsInlineHunkValue(value));
    }

    char* begin = Data_.Preallocate(estimatedSize);
    char* current = begin;

    current += WriteVarUint32(current, static_cast<ui32>(row.GetCount()));
    for (auto value : row) {
        if (value.Type == EValueType::Composite) {
            value.Type = EValueType::Any;
        }
        current += WriteRowValue(current, value, IsInlineHunkValue(value));
    }

    auto writtenSize = current - begin;
    YT_VERIFY(writtenSize <= estimatedSize);
    Data_.Advance(writtenSize);
}

TBlock THorizontalBlockWriter::FlushBlock()
{
    YT_VERIFY(!Closed_);

    TDataBlockMeta meta;
    meta.set_row_count(RowCount_);
    meta.set_uncompressed_size(GetBlockSize());

    std::vector<TSharedRef> blockParts;
    auto offsets = Offsets_.Finish();
    blockParts.insert(blockParts.end(), offsets.begin(), offsets.end());

    auto data = Data_.Finish();
    blockParts.insert(blockParts.end(), data.begin(), data.end());

    TBlock block;
    block.Data.swap(blockParts);
    block.Meta.Swap(&meta);

    Closed_ = true;

    return block;
}

i64 THorizontalBlockWriter::GetBlockSize() const
{
    YT_VERIFY(!Closed_);
    return Offsets_.GetSize() + Data_.GetSize();
}

i64 THorizontalBlockWriter::GetRowCount() const
{
    YT_VERIFY(!Closed_);
    return RowCount_;
}

i64 THorizontalBlockWriter::GetCapacity() const
{
    YT_VERIFY(!Closed_);
    return Offsets_.GetCapacity() + Data_.GetCapacity();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
