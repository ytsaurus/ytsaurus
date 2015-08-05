#include "stdafx.h"
#include "dynamic_memory_store_bits.h"
#include "automaton.h"
#include "tablet.h"

namespace NYT {
namespace NTabletNode {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

const int TDynamicRow::PrimaryLockIndex;
const ui32 TDynamicRow::PrimaryLockMask;
const ui32 TDynamicRow::AllLocksMask;

#endif

////////////////////////////////////////////////////////////////////////////////

TOwningKey RowToKey(
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TDynamicRow row)
{
    TUnversionedOwningRowBuilder builder;
    ui32 nullKeyBit = 1;
    ui32 nullKeyMask = row.GetNullKeyMask();
    const auto* srcKey = row.BeginKeys();
    auto columnIt = schema.Columns().begin();
    for (int index = 0;
         index < keyColumns.size();
         ++index, nullKeyBit <<= 1, ++srcKey, ++columnIt)
    {
        TUnversionedValue dstKey;
        dstKey.Id = index;
        if (nullKeyMask & nullKeyBit) {
            dstKey.Type = EValueType::Null;
        } else {
            dstKey.Type = columnIt->Type;
            if (IsStringLikeType(EValueType(dstKey.Type))) {
                dstKey.Length = srcKey->String->Length;
                dstKey.Data.String = srcKey->String->Data;
            } else {
                ::memcpy(&dstKey.Data, srcKey, sizeof(TDynamicValueData));
            }
        }
        builder.AddValue(dstKey);
    }
    return builder.FinishRow();
}

TOwningKey RowToKey(
    const TTableSchema& /*schema*/,
    const TKeyColumns& keyColumns,
    TUnversionedRow row)
{
    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < keyColumns.size(); ++index) {
        builder.AddValue(row[index]);
    }
    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
