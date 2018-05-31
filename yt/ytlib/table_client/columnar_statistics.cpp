#include "columnar_statistics.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TColumnarStatistics& TColumnarStatistics::operator +=(const TColumnarStatistics& other)
{
    for (int index = 0; index < ColumnDataWeights.size(); ++index) {
        ColumnDataWeights[index] += other.ColumnDataWeights[index];
    }
    LegacyChunkDataWeight += other.LegacyChunkDataWeight;
    return *this;
}

TColumnarStatistics TColumnarStatistics::MakeEmpty(int columnCount)
{
    return TColumnarStatistics{std::vector<i64>(columnCount, 0), 0};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
