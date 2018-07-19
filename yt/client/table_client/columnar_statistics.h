#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnarStatistics
{
    //! Per-column total data weight for chunks whose meta contains columnar statistics.
    std::vector<i64> ColumnDataWeights;
    //! Total weight of all write and delete timestamps.
    TNullable<i64> TimestampTotalWeight;
    //! Total data weight of legacy chunks whose meta misses columnar statistics.
    i64 LegacyChunkDataWeight = 0;

    TColumnarStatistics& operator +=(const TColumnarStatistics& other);

    static TColumnarStatistics MakeEmpty(int columnCount);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
