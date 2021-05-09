#include "sorted_dynamic_comparer.h"
#include "private.h"
#include "dynamic_store_bits.h"
#include "row_comparer_generator.h"

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NTabletNode {

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
    TRange<EValueType> keyColumnTypes)
{
    TCGFunction<TDDComparerSignature> ddComparer;
    TCGFunction<TDUComparerSignature> duComparer;
    TCGFunction<TUUComparerSignature> uuComparer;
    std::tie(ddComparer, duComparer, uuComparer) = GenerateComparers(keyColumnTypes);
    return TSortedDynamicRowKeyComparer(
        keyColumnTypes.Size(),
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
    YT_ASSERT(static_cast<int>(rhs.Row.GetCount()) >= KeyColumnCount_);
    return DUComparer_(
        lhs.GetNullKeyMask(),
        lhs.BeginKeys(),
        rhs.Row.Begin(),
        KeyColumnCount_);
}

int TSortedDynamicRowKeyComparer::operator()(TSortedDynamicRow lhs, TVersionedRowWrapper rhs) const
{
    YT_ASSERT(rhs.Row.GetKeyCount() == KeyColumnCount_);
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

class TCachedRowComparer
    : public TSyncCacheValueBase<TKeyColumnTypes, TCachedRowComparer, THash<TRange<EValueType>>>
    , public TSortedDynamicRowKeyComparer
{
public:
    TCachedRowComparer(
        TKeyColumnTypes keyColumnTypes,
        TSortedDynamicRowKeyComparer comparer)
        : TSyncCacheValueBase(keyColumnTypes)
        , TSortedDynamicRowKeyComparer(std::move(comparer))
    { }
};

class TRowComparerCache
    : public TSyncSlruCacheBase<TKeyColumnTypes, TCachedRowComparer, THash<TRange<EValueType>>>
    , public IRowComparerProvider
{
public:
    using TSyncSlruCacheBase::TSyncSlruCacheBase;

    virtual TSortedDynamicRowKeyComparer Get(TKeyColumnTypes keyColumnTypes) override
    {
        auto cachedEvaluator = TSyncSlruCacheBase::Find(keyColumnTypes);
        if (!cachedEvaluator) {
            cachedEvaluator = New<TCachedRowComparer>(
                keyColumnTypes,
                TSortedDynamicRowKeyComparer::Create(keyColumnTypes));

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return *cachedEvaluator;
    }

};

IRowComparerProviderPtr CreateRowComparerProvider(TSlruCacheConfigPtr config)
{
    return New<TRowComparerCache>(config);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
