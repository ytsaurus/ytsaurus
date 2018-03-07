#include "sorted_dynamic_comparer.h"
#include "private.h"
#include "dynamic_store_bits.h"
#include "row_comparer_generator.h"

namespace NYT {
namespace NTabletNode {

using namespace NCodegen;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TSortedDynamicRowKeyComparer::TSortedDynamicRowKeyComparer(
    int keyColumnCount,
    TCGFunction<TDDComparerSignature> ddComparer,
    TCGFunction<TDUComparerSignature> duComparer,
    TCGFunction<TUUComparerSignature> uuComparer)
    : KeyColumnCount_(keyColumnCount)
    , DDComparer_(std::move(ddComparer))
    , DUComparer_(std::move(duComparer))
    , UUComparer_(std::move(uuComparer))
{ }

TSortedDynamicRowKeyComparer TSortedDynamicRowKeyComparer::Create(
    int keyColumnCount,
    const TTableSchema& schema)
{
    TCGFunction<TDDComparerSignature> ddComparer;
    TCGFunction<TDUComparerSignature> duComparer;
    TCGFunction<TUUComparerSignature> uuComparer;
    std::tie(ddComparer, duComparer, uuComparer) = GenerateComparers(keyColumnCount, schema);
    return TSortedDynamicRowKeyComparer(
        keyColumnCount,
        std::move(ddComparer),
        std::move(duComparer),
        std::move(uuComparer));
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
{
    return DDComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.GetNullKeyMask(),
        rhs.BeginKeys());
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TUnversionedRowWrapper rhs) const
{
    Y_ASSERT(rhs.Row.GetCount() >= KeyColumnCount_);
    return DUComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.Row.Begin(),
        KeyColumnCount_);
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TVersionedRowWrapper rhs) const
{
    Y_ASSERT(rhs.Row.GetKeyCount() == KeyColumnCount_);
    return DUComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.Row.BeginKeys(),
        KeyColumnCount_);
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TKeyWrapper rhs) const
{
    return DUComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.Row.Begin(),
        rhs.Row.GetCount());
}

int TSortedDynamicRowKeyComparer::operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
{
    return operator()(lhs.Begin(), lhs.End(), rhs.Begin(), rhs.End());
}

int TSortedDynamicRowKeyComparer::operator()(
    const TUnversionedValue* lhsBegin,
    const TUnversionedValue* lhsEnd,
    const TUnversionedValue* rhsBegin,
    const TUnversionedValue* rhsEnd) const
{
    return UUComparer_(lhsBegin, lhsEnd - lhsBegin, rhsBegin, rhsEnd - rhsBegin);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
