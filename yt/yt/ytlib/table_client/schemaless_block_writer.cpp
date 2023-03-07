#include "schemaless_block_writer.h"

namespace NYT::NTableClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

struct THorizontalSchemalessBlockWriterTag { };

// NB! Must exceed lf_alloc small block size limit.
const i64 THorizontalBlockWriter::MinReserveSize = 64_KB + 1;
const i64 THorizontalBlockWriter::MaxReserveSize = 2_MB;

THorizontalBlockWriter::THorizontalBlockWriter(i64 reserveSize)
    : ReserveSize_(std::min(
        std::max(MinReserveSize, reserveSize),
        MaxReserveSize))
    , Offsets_(THorizontalSchemalessBlockWriterTag(), 4 * 1024, ReserveSize_ / 2)
    , Data_(THorizontalSchemalessBlockWriterTag(), 4 * 1024, ReserveSize_ / 2)
{ }

void THorizontalBlockWriter::WriteRow(TUnversionedRow row)
{
    YT_VERIFY(!Closed_);

    ++RowCount_;

    WritePod(Offsets_, static_cast<ui32>(Data_.GetSize()));

    int size = MaxVarUint32Size;
    for (auto it = row.Begin(); it != row.End(); ++it) {
        size += GetByteSize(*it);
    }

    char* begin = Data_.Preallocate(size);
    char* current = begin;

    current += WriteVarUint32(current, static_cast<ui32>(row.GetCount()));
    for (const auto& value : row) {
        if (value.Type == EValueType::Composite) {
            auto valueCopy = value;
            valueCopy.Type = EValueType::Any;
            current += WriteValue(current, valueCopy);
        } else {
            current += WriteValue(current, value);
        }
    }

    Data_.Advance(current - begin);
}

TBlock THorizontalBlockWriter::FlushBlock()
{
    YT_VERIFY(!Closed_);

    TBlockMeta meta;
    meta.set_row_count(RowCount_);
    meta.set_uncompressed_size(GetBlockSize());

    std::vector<TSharedRef> blockParts;
    auto offsets = Offsets_.Flush();
    blockParts.insert(blockParts.end(), offsets.begin(), offsets.end());

    auto data = Data_.Flush();
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
