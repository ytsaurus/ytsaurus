#pragma once

#include <yt/core/misc/common.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnarStatistics
{
    //! Per-column total data weight for chunks whose meta contains columnar statistics.
    std::vector<i64> ColumnDataWeights;
    //! Total data weight of legacy chunks whose meta misses columnar statistics.
    i64 LegacyChunkDataWeight = 0;

    TColumnarStatistics& operator +=(const TColumnarStatistics& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
