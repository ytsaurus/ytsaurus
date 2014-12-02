#include "stdafx.h"
#include "dynamic_memory_store_bits.h"
#include "automaton.h"
#include "tablet.h"

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;

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

void SaveRowKeys(
    TSaveContext& context,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TDynamicRow row)
{
    ui32 nullKeyMask = row.GetNullKeyMask();
    ui32 nullKeyBit = 1;
    const auto* key = row.BeginKeys();
    auto columnIt = schema.Columns().begin();
    Save(context, nullKeyMask);
    for (int index = 0;
         index < keyColumns.size();
         ++index, nullKeyBit <<= 1, ++key, ++columnIt)
    {
        if (!(nullKeyMask & nullKeyBit)) {
            switch (columnIt->Type) {
                case EValueType::Int64:
                    Save(context, key->Int64);
                    break;

                case EValueType::Uint64:
                    Save(context, key->Uint64);
                    break;

                case EValueType::Double:
                    Save(context, key->Double);
                    break;

                case EValueType::Boolean:
                    Save(context, key->Boolean);
                    break;

                case EValueType::String:
                case EValueType::Any:
                    Save(context, key->String->Length);
                    TRangeSerializer::Save(context, TRef(key->String->Data, key->String->Length));
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }
}

void LoadRowKeys(
    TLoadContext& context,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TChunkedMemoryPool* alignedPool,
    TDynamicRow row)
{
    ui32 nullKeyMask = Load<ui32>(context);
    row.SetNullKeyMask(nullKeyMask);
    ui32 nullKeyBit = 1;
    auto* key = row.BeginKeys();
    auto columnIt = schema.Columns().begin();
    for (int index = 0;
         index < keyColumns.size();
         ++index, nullKeyBit <<= 1, ++key, ++columnIt)
    {
        if (!(nullKeyMask & nullKeyBit)) {
            switch (columnIt->Type) {
                case EValueType::Int64:
                    key->Int64 = Load<i64>(context);
                    break;

                case EValueType::Uint64:
                    key->Uint64 = Load<ui64>(context);
                    break;

                case EValueType::Double:
                    key->Double = Load<double>(context);
                    break;

                case EValueType::Boolean:
                    key->Boolean = Load<bool>(context);
                    break;

                case EValueType::String:
                case EValueType::Any: {
                    ui32 length = Load<ui32>(context);
                    key->String = reinterpret_cast<TDynamicString*>(alignedPool->AllocateAligned(
                        length + sizeof(ui32),
                        sizeof(ui32)));
                    key->String->Length = length;
                    TRangeSerializer::Load(context, TRef(key->String->Data, length));
                    break;
                }

                default:
                    YUNREACHABLE();
            }
        }
    }
}

void LoadRowKeys(
    TLoadContext& context,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    TChunkedMemoryPool* unalignedPool,
    TUnversionedRowBuilder* builder)
{
    ui32 nullKeyMask = Load<ui32>(context);
    ui32 nullKeyBit = 1;
    auto columnIt = schema.Columns().begin();
    for (int index = 0;
         index < keyColumns.size();
         ++index, nullKeyBit <<= 1, ++columnIt)
    {
        TUnversionedValue value;
        value.Id = index;
        if (nullKeyMask & nullKeyBit) {
            value.Type = EValueType::Null;
        } else {
            value.Type = columnIt->Type;
            switch (columnIt->Type) {
                case EValueType::Int64:
                    value.Data.Int64 = Load<i64>(context);
                    break;

                case EValueType::Uint64:
                    value.Data.Uint64 = Load<ui64>(context);
                    break;

                case EValueType::Double:
                    value.Data.Double = Load<double>(context);
                    break;

                case EValueType::Boolean:
                    value.Data.Boolean = Load<bool>(context);
                    break;

                case EValueType::String:
                case EValueType::Any:
                    value.Length = Load<ui32>(context);
                    value.Data.String = unalignedPool->AllocateUnaligned(value.Length);
                    TRangeSerializer::Load(context, TRef(const_cast<char*>(value.Data.String), value.Length));
                    break;

                default:
                    YUNREACHABLE();
            }
        }
        builder->AddValue(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
