#include "sorted_dynamic_comparer.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTabletNode {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSortedDynamicRowKeyComparer::TSortedDynamicRowKeyComparer(NQueryClient::TCGKeyComparers comparers)
    : NQueryClient::TCGKeyComparers(std::move(comparers))
{ }

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
{
    return GetCompareSign(
        DDComparer(lhs.GetNullKeyMask(), lhs.BeginKeys(), rhs.GetNullKeyMask(), rhs.BeginKeys()));
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TUnversionedValueRange rhs) const
{
    return GetCompareSign(
        DUComparer(lhs.GetNullKeyMask(), lhs.BeginKeys(), rhs.Begin(), rhs.Size()));
}

int TSortedDynamicRowKeyComparer::operator()(TUnversionedValueRange lhs, TSortedDynamicRow rhs) const
{
    return -GetCompareSign(
        DUComparer(rhs.GetNullKeyMask(), rhs.BeginKeys(), lhs.Begin(), lhs.Size()));
}

int TSortedDynamicRowKeyComparer::operator()(TUnversionedValueRange lhs, TUnversionedValueRange rhs) const
{
    return CompareKeys(lhs, rhs, UUComparer);
}

int TSortedDynamicRowKeyComparer::operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
{
    return CompareKeys(ToKeyRef(lhs), ToKeyRef(rhs), UUComparer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
