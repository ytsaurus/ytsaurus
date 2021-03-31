#pragma once

#include <util/datetime/base.h>

#include <vector>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct THistogramSnapshot
{
    // When Values.size() == Times.size() + 1, Values.back() stores "Inf" bucket.
    std::vector<int> Values;
    std::vector<TDuration> Times;

    THistogramSnapshot& operator += (const THistogramSnapshot& other);

    bool operator == (const THistogramSnapshot& other) const;
    bool operator != (const THistogramSnapshot& other) const;
    bool IsEmpty() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
