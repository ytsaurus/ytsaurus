#include "stdafx.h"
#include "schemaless_block_writer.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

struct THorizontalSchemalessBlockWriterTag { };

int THorizontalSchemalessBlockWriter::FormatVersion = ETableChunkFormat::SchemalessHorizontal;

THorizontalSchemalessBlockWriter::THorizontalSchemalessBlockWriter()
    : RowCount_(0)
    , Offsets_(THorizontalSchemalessBlockWriterTag())
    , Data_(THorizontalSchemalessBlockWriterTag())
{ }

THorizontalSchemalessBlockWriter::THorizontalSchemalessBlockWriter(int /* keyColumns */)
    : THorizontalSchemalessBlockWriter()
{ }

void THorizontalSchemalessBlockWriter::WriteRow(TUnversionedRow row)
{
    ++RowCount_;

    WritePod(Offsets_, static_cast<ui32>(Data_.GetSize()));

    int size = 0;
    for (auto it = row.Begin(); it != row.End(); ++it) {
        size += GetByteSize(*it);
    }

    char* begin = Data_.Preallocate(size);
    char* current = begin;
    for (auto it = row.Begin(); it != row.End(); ++it) {
        current += WriteValue(current, *it);
    }

    Data_.Advance(current - begin);
}

TBlock THorizontalSchemalessBlockWriter::FlushBlock()
{
    std::vector<TSharedRef> blockParts;
    auto offsets = Offsets_.Flush();
    blockParts.insert(blockParts.end(), offsets.begin(), offsets.end());

    auto data = Data_.Flush();
    blockParts.insert(blockParts.end(), data.begin(), data.end());

    TBlockMeta meta;
    meta.set_row_count(RowCount_);
    meta.set_uncompressed_size(GetBlockSize());

    TBlock block;
    block.Data.swap(blockParts);
    block.Meta.Swap(&meta);

    return block;
}

i64 THorizontalSchemalessBlockWriter::GetBlockSize() const
{
    return Offsets_.GetSize() + Data_.GetSize();
}

i64 THorizontalSchemalessBlockWriter::GetRowCount() const
{
    return RowCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
