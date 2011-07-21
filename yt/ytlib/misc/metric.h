#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetric
    : private TNonCopyable
{
public:
    TMetric(double minValue, double maxValue, int bucketCount);

    //! Adds value to analysis.
    void AddValue(double value);

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
    int Delta;

    //! Contains number of values from (-inf, #MinValue).
    int MinBucket;

    //! Contains number of values from (#MaxValue, inf).
    int MaxBucket;

    //! Contains number of values from [#MinValue, #MaxValue]
    yvector<int> Buckets;

    //! Puts the #value into an appropriate bucket.
    void IncrementBucket(double value);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
