#include "row_comparer_generator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TCGKeyComparers GenerateComparers(TRange<EValueType> /*keyColumnTypes*/)
{
    // Proper implementation resides in yt/yt/library/query/row_comparer/row_comparer_generator.cpp.
    THROW_ERROR_EXCEPTION("Error query/row_comparer library is not linked; consider PEERDIR'ing yt/yt/library/query/row_comparer");
}

Y_WEAK IRowComparerProviderPtr CreateRowComparerProvider(TSlruCacheConfigPtr /*config*/)
{
    // Proper implementation resides in yt/yt/library/query/row_comparer/row_comparer_generator.cpp.
    THROW_ERROR_EXCEPTION("Error query/row_comparer library is not linked; consider PEERDIR'ing yt/yt/library/query/row_comparer");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
