#include "dynamic_store_bits.h"

#include "sorted_dynamic_comparer.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue GetUnversionedKeyValue(TSortedDynamicRow row, int index, EValueType type)
{
    auto nullKeyMask = row.GetNullKeyMask();
    YT_ASSERT(index < static_cast<int>(sizeof(nullKeyMask) * 8));

    auto source = row.BeginKeys()[index];

    auto result = MakeUnversionedSentinelValue(EValueType::Null, index);
    if (!(nullKeyMask & (static_cast<TDynamicTableKeyMask>(1) << index))) {
        result.Type = type;
        if (IsStringLikeType(type)) {
            result.Length = source.String->Length;
            result.Data.String = source.String->Data;
        } else {
            ::memcpy(&result.Data, &source, sizeof(TDynamicValueData));
        }
    }

    return result;
}

bool TLockDescriptor::HasUnpreparedSharedWriteTransaction(TTransaction* transaction) const
{
    return SharedWriteTransactions.contains({NTableClient::NotPreparedTimestamp, transaction});
}

////////////////////////////////////////////////////////////////////////////////

TSortedDynamicRowKeyEq::TSortedDynamicRowKeyEq(const TSortedDynamicRowKeyComparer* rowKeyComparer)
    : RowKeyComparer_(rowKeyComparer)
{ }

bool TSortedDynamicRowKeyEq::operator()(
    const TSchemafulSortedDynamicRow& lhsSchemafulRow,
    const TLegacyOwningKey& rhsOwningKey) const
{
    return (*RowKeyComparer_)(lhsSchemafulRow.Row, rhsOwningKey.Elements()) == 0;
}

bool TSortedDynamicRowKeyEq::operator()(
    const TLegacyOwningKey& lhsOwningKey,
    const TLegacyOwningKey& rhsOwningKey) const
{
    return (*RowKeyComparer_)(lhsOwningKey, rhsOwningKey) == 0;
}

bool TSortedDynamicRowKeyEq::operator()(
    const TLegacyOwningKey& lhsOwningKey,
    const TSchemafulSortedDynamicRow& rhsSchemafulRow) const
{
    return (*RowKeyComparer_)(lhsOwningKey.Elements(), rhsSchemafulRow.Row) == 0;
}

bool TSortedDynamicRowKeyEq::operator()(
    const TSchemafulSortedDynamicRow& lhsSchemafulRow,
    const TSchemafulSortedDynamicRow& rhsSchemafulRow) const
{
    return (*RowKeyComparer_)(lhsSchemafulRow.Row, rhsSchemafulRow.Row) == 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
