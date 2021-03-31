#include "histogram_snapshot.h"

#include <yt/yt/core/misc/assert.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

THistogramSnapshot MergeHistograms(const THistogramSnapshot& first, const THistogramSnapshot& second)
{
    THistogramSnapshot result;

    size_t i = 0, j = 0;
    auto pushFirst = [&] {
        result.Times.push_back(first.Times[i]);
        result.Values.push_back(first.Values[i]);
        ++i;
    };
    auto pushSecond = [&] {
        result.Times.push_back(second.Times[j]);
        result.Values.push_back(second.Values[j]);
        ++j;
    };

    while (true) {
        if (i == first.Times.size() || j == second.Times.size()) {
            break;
        }

        if (first.Times[i] < second.Times[j]) {
            pushFirst();
        } else if (first.Times[i] > second.Times[j]) {
            pushSecond();
        } else {
            result.Times.push_back(second.Times[j]);
            result.Values.push_back(first.Values[i] + second.Values[j]);

            ++i;
            ++j;
        }
    }

    while (i != first.Times.size()) {
        pushFirst();
    }

    while (j != second.Times.size()) {
        pushSecond();
    }

    int infBucket = 0;
    if (first.Values.size() != first.Times.size()) {
        infBucket += first.Values.back();
    }
    if (second.Values.size() != second.Times.size()) {
        infBucket += second.Values.back();
    }
    if (infBucket != 0) {
        result.Values.push_back(infBucket);
    }

    return result;
}

THistogramSnapshot& THistogramSnapshot::operator += (const THistogramSnapshot& other)
{
    if (Times.empty()) {
        Times = other.Times;
        Values = other.Values;
    } else if (other.Times.empty()) {
        // Do nothing
    } else if (Times == other.Times) {
        if (Values.size() < other.Values.size()) {
            Values.push_back(0);
            YT_VERIFY(Values.size() == other.Values.size());
        }

        for (size_t i = 0; i < other.Values.size(); ++i) {
            Values[i] += other.Values[i];
        }
    } else {
        *this = MergeHistograms(*this, other);
    }

    return *this;
}

bool THistogramSnapshot::IsEmpty() const
{
    bool empty = true;
    for (auto value : Values) {
        if (value != 0) {
            empty = false;
        }
    }
    return empty;
}

bool THistogramSnapshot::operator == (const THistogramSnapshot& other) const
{
    if (IsEmpty() && other.IsEmpty()) {
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
