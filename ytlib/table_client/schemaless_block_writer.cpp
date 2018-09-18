#include "schemaless_block_writer.h"

namespace NYT {
namespace NTableClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

struct THorizontalSchemalessBlockWriterTag { };

// NB! Must exceed lf_alloc small block size limit.
const i64 THorizontalSchemalessBlockWriter::MinReserveSize = 64_KB + 1;
const i64 THorizontalSchemalessBlockWriter::MaxReserveSize = 2_MB;

THorizontalSchemalessBlockWriter::THorizontalSchemalessBlockWriter(i64 reserveSize)
    : RowCount_(0)
    , Closed_(false)
    , ReserveSize_(std::min(
        std::max(MinReserveSize, reserveSize),
        MaxReserveSize))
    , Offsets_(THorizontalSchemalessBlockWriterTag(), 4 * 1024, ReserveSize_ / 2)
    , Data_(THorizontalSchemalessBlockWriterTag(), 4 * 1024, ReserveSize_ / 2)
{ }

void THorizontalSchemalessBlockWriter::WriteRow(TUnversionedRow row)
{
    YCHECK(!Closed_);

    ++RowCount_;

    WritePod(Offsets_, static_cast<ui32>(Data_.GetSize()));

    int size = MaxVarUint32Size;
    for (auto it = row.Begin(); it != row.End(); ++it) {
        size += GetByteSize(*it);
    }

    char* begin = Data_.Preallocate(size);
    char* current = begin;

    current += WriteVarUint32(current, static_cast<ui32>(row.GetCount()));
    for (auto it = row.Begin(); it != row.End(); ++it) {
        current += WriteValue(current, *it);
    }

    Data_.Advance(current - begin);
}

TBlock THorizontalSchemalessBlockWriter::FlushBlock()
{
    YCHECK(!Closed_);

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

i64 THorizontalSchemalessBlockWriter::GetBlockSize() const
{
    YCHECK(!Closed_);
    return Offsets_.GetSize() + Data_.GetSize();
}

i64 THorizontalSchemalessBlockWriter::GetRowCount() const
{
    YCHECK(!Closed_);
    return RowCount_;
}

i64 THorizontalSchemalessBlockWriter::GetCapacity() const
{
    YCHECK(!Closed_);
    return Offsets_.GetCapacity() + Data_.GetCapacity();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
