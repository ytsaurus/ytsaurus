#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetric
{
public:
    TMetric(double minValue, double maxValue, int bucketCount);

    //! Adds value to analysis.
    void AddValue(double value);

    //! Adds duraion in milliseconds to analysis.
    void AddValue(TDuration duration);

    //! Adds time passed from #start to now to analysis.
    void AddDelta(TInstant start);

    //! Returns mean of all values.
    double GetMean() const;

    //! Returns standart deviation of all values.
    double GetStd() const;

    //! Returns information about all values.
    Stroka GetDebugInfo() const;

private:
    double MinValue;
    double MaxValue;
    int BucketCount;

    //! Number of added values.
    int ValueCount;

    //! Sum of added values.
    double Sum;

    //! Sum of squares of added values.
    double SumSquares;

    //! The length of segment correpsonding to one bucket.
    double Delta;

    //! Contains number of values from (-inf, #MinValue).
    int MinBucket;

    //! Contains number of values from [#MaxValue, +inf).
    int MaxBucket;

    //! Contains number of values from [#MinValue, #MaxValue)
    std::vector<int> Buckets;

    //! Puts the #value into an appropriate bucket.
    void IncrementBucket(double value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
