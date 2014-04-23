#include "stdafx.h"
#include "schemaless_block_writer.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

struct THorizontalSchemalessBlockWriterTag { };

THorizontalSchemalessBlockWriter::THorizontalSchemalessBlockWriter()
    : RowCount_(0)
    , Closed_(false)
    , Offsets_(THorizontalSchemalessBlockWriterTag())
    , Data_(THorizontalSchemalessBlockWriterTag())
{ }

void THorizontalSchemalessBlockWriter::WriteRow(TUnversionedRow row)
{
    YCHECK(!Closed_);

    ++RowCount_;

    WritePod(Offsets_, static_cast<ui32>(Data_.GetSize()));

    int size = MaxVarUInt32Size;
    for (auto it = row.Begin(); it != row.End(); ++it) {
        size += GetByteSize(*it);
    }

    char* begin = Data_.Preallocate(size);
    char* current = begin;

    current += WriteVarUInt32(current, static_cast<ui32>(row.GetCount()));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
