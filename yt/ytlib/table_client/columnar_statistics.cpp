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

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
