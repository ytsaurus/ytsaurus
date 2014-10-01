#include "stdafx.h"
#include "dynamic_memory_store_bits.h"
#include "automaton.h"
#include "tablet.h"

namespace NYT {
namespace NTabletNode {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

const int TLockDescriptor::InvalidRowIndex;

////////////////////////////////////////////////////////////////////////////////

const int TDynamicRow::PrimaryLockIndex;
const ui32 TDynamicRow::PrimaryLockMask;
const ui32 TDynamicRow::AllLocksMask;

////////////////////////////////////////////////////////////////////////////////

TDynamicRowKeyComparer::TDynamicRowKeyComparer(int keyColumnCount, const TTableSchema& schema)
    : KeyColumnCount_(keyColumnCount)
    , Schema_(schema)
{ }

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TDynamicRow rhs) const
{
    ui32 nullKeyBit = 1;
    ui32 lhsNullKeyMask = lhs.GetNullKeyMask();
    ui32 rhsNullKeyMask = rhs.GetNullKeyMask();
    const auto* lhsValue = lhs.BeginKeys();
    const auto* rhsValue = rhs.BeginKeys();
    auto columnIt = Schema_.Columns().begin();
    for (int index = 0;
         index < KeyColumnCount_;
         ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
    {
        bool lhsNull = (lhsNullKeyMask & nullKeyBit);
        bool rhsNull = (rhsNullKeyMask & nullKeyBit);
        if (lhsNull && !rhsNull) {
            return -1;
        } else if (!lhsNull && rhsNull) {
            return +1;
        } else if (lhsNull && rhsNull) {
            continue;
        }

        switch (columnIt->Type) {
            case EValueType::Int64: {
                i64 lhsData = lhsValue->Int64;
                i64 rhsData = rhsValue->Int64;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::Uint64: {
                ui64 lhsData = lhsValue->Uint64;
                ui64 rhsData = rhsValue->Uint64;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::Double: {
                double lhsData = lhsValue->Double;
                double rhsData = rhsValue->Double;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::Boolean: {
                bool lhsData = lhsValue->Boolean;
                bool rhsData = rhsValue->Boolean;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::String: {
                size_t lhsLength = lhsValue->String->Length;
                size_t rhsLength = rhsValue->String->Length;
                size_t minLength = std::min(lhsLength, rhsLength);
                int result = ::memcmp(lhsValue->String->Data, rhsValue->String->Data, minLength);
                if (result != 0) {
                    return result;
                } else if (lhsLength < rhsLength) {
                    return -1;
                } else if (lhsLength > rhsLength) {
                    return +1;
                }
                break;
            }

            default:
                YUNREACHABLE();
        }
    }
    return 0;
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TRowWrapper rhs) const
{
    YASSERT(rhs.Row.GetCount() >= KeyColumnCount_);
    return Compare(lhs,rhs.Row.Begin(),KeyColumnCount_);
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TKeyWrapper rhs) const
{
    return Compare(lhs, rhs.Row.Begin(), rhs.Row.GetCount());
}

int TDynamicRowKeyComparer::Compare(TDynamicRow lhs, TUnversionedValue* rhsBegin, int rhsLength) const
{
    ui32 nullKeyBit = 1;
    ui32 lhsNullKeyMask = lhs.GetNullKeyMask();
    const auto* lhsValue = lhs.BeginKeys();
    const auto* rhsValue = rhsBegin;
    auto columnIt = Schema_.Columns().begin();
    int lhsLength = KeyColumnCount_;
    int minLength = std::min(lhsLength, rhsLength);
    for (int index = 0;
         index < minLength;
         ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
    {
        auto lhsType = (lhsNullKeyMask & nullKeyBit) ? EValueType(EValueType::Null) : columnIt->Type;
        if (lhsType < rhsValue->Type) {
            return -1;
        } else if (lhsType > rhsValue->Type) {
            return +1;
        }

        switch (lhsType) {
            case EValueType::Int64: {
                i64 lhsData = lhsValue->Int64;
                i64 rhsData = rhsValue->Data.Int64;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::Uint64: {
                ui64 lhsData = lhsValue->Uint64;
                ui64 rhsData = rhsValue->Data.Uint64;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::Double: {
                double lhsData = lhsValue->Double;
                double rhsData = rhsValue->Data.Double;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::Boolean: {
                bool lhsData = lhsValue->Boolean;
                bool rhsData = rhsValue->Data.Boolean;
                if (lhsData < rhsData) {
                    return -1;
                } else if (lhsData > rhsData) {
                    return +1;
                }
                break;
            }

            case EValueType::String: {
                size_t lhsLength = lhsValue->String->Length;
                size_t rhsLength = rhsValue->Length;
                size_t minLength = std::min(lhsLength, rhsLength);
                int result = ::memcmp(lhsValue->String->Data, rhsValue->Data.String, minLength);
                if (result != 0) {
                    return result;
                } else if (lhsLength < rhsLength) {
                    return -1;
                } else if (lhsLength > rhsLength) {
                    return +1;
                }
                break;
            }

            case EValueType::Null:
                break;

            default:
                YUNREACHABLE();
        }
    }
    return lhsLength - rhsLength;
}

////////////////////////////////////////////////////////////////////////////////

void SaveRowKeys(
    TSaveContext& context,
    TDynamicRow row,
    TTablet* tablet)
{
    ui32 nullKeyMask = row.GetNullKeyMask();
    ui32 nullKeyBit = 1;
    const auto* key = row.BeginKeys();
    auto columnIt = tablet->Schema().Columns().begin();
    Save(context, nullKeyMask);
    for (int index = 0;
         index < tablet->GetKeyColumnCount();
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
    TDynamicRow row,
    TTablet* tablet,
    TChunkedMemoryPool* alignedPool)
{
    ui32 nullKeyMask = Load<ui32>(context);
    row.SetNullKeyMask(nullKeyMask);
    ui32 nullKeyBit = 1;
    auto* key = row.BeginKeys();
    auto columnIt = tablet->Schema().Columns().begin();
    for (int index = 0;
         index < tablet->GetKeyColumnCount();
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
    TUnversionedRowBuilder* builder,
    TTablet* tablet,
    TChunkedMemoryPool* unalignedPool)
{
    ui32 nullKeyMask = Load<ui32>(context);
    ui32 nullKeyBit = 1;
    auto columnIt = tablet->Schema().Columns().begin();
    for (int index = 0;
         index < tablet->GetKeyColumnCount();
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
