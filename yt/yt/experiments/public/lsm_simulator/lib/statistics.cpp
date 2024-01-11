#include "statistics.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

TStatistics& operator+= (TStatistics& lhs, const TStatistics& rhs)
{
    lhs.BytesWritten += rhs.BytesWritten;
    lhs.BytesFlushed += rhs.BytesFlushed;
    lhs.BytesCompacted += rhs.BytesCompacted;
    lhs.BytesPartitioned += rhs.BytesPartitioned;
    return lhs;
}

TStatistics operator+ (TStatistics lhs, const TStatistics& rhs)
{
    return lhs += rhs;
}

TStatistics& operator-= (TStatistics& lhs, const TStatistics& rhs)
{
    lhs.BytesWritten -= rhs.BytesWritten;
    lhs.BytesFlushed -= rhs.BytesFlushed;
    lhs.BytesCompacted -= rhs.BytesCompacted;
    lhs.BytesPartitioned -= rhs.BytesPartitioned;
    return lhs;
}

TStatistics operator- (TStatistics lhs, const TStatistics& rhs)
{
    return lhs -= rhs;
}

TStatistics operator/ (TStatistics lhs, int rhs)
{
    lhs.BytesWritten /= rhs;
    lhs.BytesFlushed /= rhs;
    lhs.BytesCompacted /= rhs;
    lhs.BytesPartitioned /= rhs;
    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
