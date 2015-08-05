#include "dynamic_memory_store_bits.h"
#include "dynamic_memory_store_comparer.h"
#include "row_comparer_generator.h"
#include "private.h"

namespace NYT {
namespace NTabletNode {

using namespace NCodegen;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDynamicRowKeyComparer::TDynamicRowKeyComparer(
    int keyColumnCount,
    TCGFunction<TDDComparerSignature> ddComparer,
    TCGFunction<TDUComparerSignature> duComparer,
    TCGFunction<TUUComparerSignature> uuComparer)
    : KeyColumnCount_(keyColumnCount)
    , DDComparer_(std::move(ddComparer))
    , DUComparer_(std::move(duComparer))
    , UUComparer_(std::move(uuComparer))
{ }

TDynamicRowKeyComparer TDynamicRowKeyComparer::Create(
    int keyColumnCount,
    const TTableSchema& schema)
{
    TCGFunction<TDDComparerSignature> ddComparer;
    TCGFunction<TDUComparerSignature> duComparer;
    TCGFunction<TUUComparerSignature> uuComparer;
    std::tie(ddComparer, duComparer, uuComparer) = GenerateComparers(keyColumnCount, schema);
    return TDynamicRowKeyComparer(
        keyColumnCount,
        std::move(ddComparer),
        std::move(duComparer),
        std::move(uuComparer));
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TDynamicRow rhs) const
{
    return DDComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.GetNullKeyMask(),
        rhs.BeginKeys());
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TRowWrapper rhs) const
{
    YASSERT(rhs.Row.GetCount() >= KeyColumnCount_);
    return DUComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.Row.Begin(),
        KeyColumnCount_);
}

int TDynamicRowKeyComparer::operator()(TDynamicRow lhs, TKeyWrapper rhs) const
{
    return DUComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.Row.Begin(),
        rhs.Row.GetCount());
}

int TDynamicRowKeyComparer::operator()(
    const TUnversionedValue* lhsBegin,
    const TUnversionedValue* lhsEnd,
    const TUnversionedValue* rhsBegin,
    const TUnversionedValue* rhsEnd) const
{
    YCHECK(lhsEnd - lhsBegin == KeyColumnCount_);
    YCHECK(rhsEnd - rhsBegin == KeyColumnCount_);
    return UUComparer_(lhsBegin, rhsBegin);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
