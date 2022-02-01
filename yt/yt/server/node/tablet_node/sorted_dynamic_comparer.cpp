#include "sorted_dynamic_comparer.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTabletNode {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TRange<TUnversionedValue> ToKeyRef(TVersionedRow row)
{
    return MakeRange(row.BeginKeys(), row.EndKeys());
}

TSortedDynamicRowKeyComparer::TSortedDynamicRowKeyComparer(NTabletClient::TCGKeyComparers comparers)
    : NTabletClient::TCGKeyComparers(std::move(comparers))
{ }

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
{
    return GetCompareSign(
        DDComparer(lhs.GetNullKeyMask(), lhs.BeginKeys(), rhs.GetNullKeyMask(), rhs.BeginKeys()));
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TRange<TUnversionedValue> rhs) const
{
    return GetCompareSign(
        DUComparer(lhs.GetNullKeyMask(), lhs.BeginKeys(), rhs.Begin(), rhs.Size()));
}

int TSortedDynamicRowKeyComparer::operator()(TRange<TUnversionedValue> lhs, TRange<TUnversionedValue> rhs) const
{
    return CompareKeys(lhs, rhs, UUComparer.Get());
}

int TSortedDynamicRowKeyComparer::operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
{
    return CompareKeys(ToKeyRef(lhs), ToKeyRef(rhs), UUComparer.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
