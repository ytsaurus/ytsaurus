#include "sorted_dynamic_comparer.h"
#include "private.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TRange<TUnversionedValue> ToKeyRef(TUnversionedRow row)
{
    return MakeRange(row.Begin(), row.End());
}

TRange<TUnversionedValue> ToKeyRef(TUnversionedRow row, int prefix)
{
    YT_VERIFY(prefix <= static_cast<int>(row.GetCount()));
    return MakeRange(row.Begin(), prefix);
}

TRange<TUnversionedValue> ToKeyRef(TVersionedRow row)
{
    return MakeRange(row.BeginKeys(), row.EndKeys());
}

TSortedDynamicRowKeyComparer::TSortedDynamicRowKeyComparer(NTabletClient::TCGKeyComparers comparers)
    : NTabletClient::TCGKeyComparers(std::move(comparers))
{ }

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
{
    return DDComparer(lhs.GetNullKeyMask(), lhs.BeginKeys(), rhs.GetNullKeyMask(), rhs.BeginKeys());
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TRange<TUnversionedValue> rhs) const
{
    return DUComparer(lhs.GetNullKeyMask(), lhs.BeginKeys(), rhs.Begin(), rhs.Size());
}

int TSortedDynamicRowKeyComparer::operator()(TRange<TUnversionedValue> lhs, TRange<TUnversionedValue> rhs) const
{
    return UUComparer(lhs.Begin(), lhs.Size(), rhs.Begin(), rhs.Size());
}

int TSortedDynamicRowKeyComparer::operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
{
    return UUComparer(lhs.Begin(), lhs.GetCount(), rhs.Begin(), rhs.GetCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
