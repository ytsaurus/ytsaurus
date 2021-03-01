#pragma once

#include <util/datetime/base.h>

#include <vector>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct THistogramSnapshot
{
    std::vector<TDuration> Times;
    std::vector<int> Values;

    THistogramSnapshot& operator += (const THistogramSnapshot& other);

    bool operator == (const THistogramSnapshot& other) const;
    bool operator != (const THistogramSnapshot& other) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
