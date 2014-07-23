#include "stdafx.h"
#include "versioned_block_writer.h"

#include <ytlib/transaction_client/public.h>

#include <core/misc/serialize.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const i64 NullValue = 0;

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockWriter::TSimpleVersionedBlockWriter(
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : MinTimestamp_(MaxTimestamp)
    , MaxTimestamp_(MinTimestamp)
    , SchemaColumnCount_(schema.Columns().size())
    , KeyColumnCount_(keyColumns.size())
    , TimestampCount_(0)
    , ValueCount_(0)
    , RowCount_(0)
{ }

void TSimpleVersionedBlockWriter::WriteRow(
    TVersionedRow row,
    const TUnversionedValue* /* beginPrevKey */,
    const TUnversionedValue* /* endPrevKey */)
{
    ++RowCount_;

    int keyOffset = KeyStream_.GetSize();
    for (auto* it = row.BeginKeys(); it != row.EndKeys(); ++it) {
        WriteValue(KeyStream_, KeyNullFlags_, *it);
    }

    WritePod(KeyStream_, TimestampCount_);
    WritePod(KeyStream_, ValueCount_);
    WritePod(KeyStream_, static_cast<ui32>(row.GetTimestampCount()));

    TimestampCount_ += row.GetTimestampCount();
    for (auto* it = row.BeginTimestamps(); it != row.EndTimestamps(); ++it) {
        WritePod(TimestampsStream_, *it);
    }

    ValueCount_ += row.GetValueCount();

    int lastId = KeyColumnCount_;
    ui32 valueCount = 0;
    while (valueCount < row.GetValueCount()) {
        auto& value = row.BeginValues()[valueCount];
        YASSERT(lastId <= value.Id);
        if (lastId < value.Id) {
            WritePod(KeyStream_, valueCount);
            ++lastId;
        } else {
            WriteValue(ValueStream_, ValueNullFlags_, value);
            WritePod(ValueStream_, value.Timestamp);
            ++valueCount;
        }

        auto timestamp = value.Timestamp & TimestampValueMask;
        MaxTimestamp_ = std::max(MaxTimestamp_, timestamp);
        MinTimestamp_ = std::min(MinTimestamp_, timestamp);
    }

    while (lastId < SchemaColumnCount_) {
        WritePod(KeyStream_, valueCount);
        ++lastId;
    }

    YASSERT(KeyStream_.GetSize() - keyOffset == GetKeySize(KeyColumnCount_, SchemaColumnCount_));
    WritePadding(KeyStream_, GetKeySize(KeyColumnCount_, SchemaColumnCount_));
}

TBlock TSimpleVersionedBlockWriter::FlushBlock()
{
    std::vector<TSharedRef> blockParts;
    auto keys = KeyStream_.Flush();
    blockParts.insert(blockParts.end(), keys.begin(), keys.end());

    auto values = ValueStream_.Flush();
    blockParts.insert(blockParts.end(), values.begin(), values.end());

    auto timestamps = TimestampsStream_.Flush();
    blockParts.insert(blockParts.end(), timestamps.begin(), timestamps.end());

    blockParts.insert(blockParts.end(), KeyNullFlags_.Flush());
    blockParts.insert(blockParts.end(), ValueNullFlags_.Flush());

    auto strings = StringData_.Flush();
    blockParts.insert(blockParts.end(), strings.begin(), strings.end());


    int size = 0;
    for (auto& part : blockParts) {
        size += part.Size();
    }

    TBlockMeta meta;
    meta.set_row_count(RowCount_);
    meta.set_block_size(size);

    auto* metaExt = meta.MutableExtension(TSimpleVersionedBlockMeta::block_meta_ext);
    metaExt->set_value_count(ValueCount_);
    metaExt->set_timestamp_count(TimestampCount_);

    TBlock block;
    block.Data.swap(blockParts);
    block.Meta.Swap(&meta);

    return block;
}

void TSimpleVersionedBlockWriter::WriteValue(
    TChunkedOutputStream& stream,
    TBitmap& nullFlags,
    const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Int64:
            WritePod(stream, value.Data.Int64);
            nullFlags.Append(false);
            break;

        case EValueType::Double:
            WritePod(stream, value.Data.Double);
            nullFlags.Append(false);
            break;

        case EValueType::String:
        case EValueType::Any:
            WritePod(stream, static_cast<ui32>(StringData_.GetSize()));
            WritePod(stream, value.Length);
            StringData_.Write(value.Data.String, value.Length);
            nullFlags.Append(false);
            break;

        case EValueType::Null:
            WritePod(stream, NullValue);
            nullFlags.Append(true);
            break;

        default:
            YUNREACHABLE();
    }
}

i64 TSimpleVersionedBlockWriter::GetBlockSize() const
{
    return
        KeyStream_.GetSize() +
        ValueStream_.GetSize() +
        TimestampsStream_.GetSize() +
        KeyNullFlags_.Size() +
        ValueNullFlags_.Size();
}

i64 TSimpleVersionedBlockWriter::GetRowCount() const
{
    return RowCount_;
}

int TSimpleVersionedBlockWriter::GetKeySize(int keyColumnCount, int schemaColumnCount)
{
    // 8 bytes for each key column + timestamp offset + value offset
    // 4 bytes for value count for each non-key column + timestamp count
    return 8 * (keyColumnCount + 2) + 4 * (schemaColumnCount - keyColumnCount + 1);
}

int TSimpleVersionedBlockWriter::GetPaddedKeySize(int keyColumnCount, int schemaColumnCount)
{
    return AlignUp(GetKeySize(keyColumnCount, schemaColumnCount));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
