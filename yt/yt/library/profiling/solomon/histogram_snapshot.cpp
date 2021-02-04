#include "histogram_snapshot.h"

#include <yt/core/misc/assert.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

THistogramSnapshot& THistogramSnapshot::operator += (const THistogramSnapshot& other)
{
    if (Values.empty()) {
        Values = other.Values;
    } else if (!other.Values.empty()) {
        YT_VERIFY(Values.size() == other.Values.size());
        for (size_t i = 0; i < other.Values.size(); ++i) {
            Values[i] += other.Values[i];
        }
    }

    return *this;
}

bool THistogramSnapshot::operator == (const THistogramSnapshot& other) const
{
    return Values == other.Values;
}

bool THistogramSnapshot::operator != (const THistogramSnapshot& other) const
{
    return Values != other.Values;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
