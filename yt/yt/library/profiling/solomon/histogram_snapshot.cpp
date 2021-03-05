#include "histogram_snapshot.h"

#include <yt/yt/core/misc/assert.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

THistogramSnapshot& THistogramSnapshot::operator += (const THistogramSnapshot& other)
{
    if (Values.empty()) {
        Times = other.Times;
        Values = other.Values;
    } else if (!other.Values.empty()) {
        YT_VERIFY(Values.size() == other.Values.size());
        YT_VERIFY(Times.size() == other.Times.size());
        YT_VERIFY(Times.front() == other.Times.front());
        YT_VERIFY(Times.back() == other.Times.back());
        for (size_t i = 0; i < other.Values.size(); ++i) {
            Values[i] += other.Values[i];
        }
    }

    return *this;
}

bool THistogramSnapshot::operator == (const THistogramSnapshot& other) const
{
    if (Values.empty() && other.Values.empty()) {
        return true;
    }
    return Values == other.Values && Times == other.Times;
}

bool THistogramSnapshot::operator != (const THistogramSnapshot& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
