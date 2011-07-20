#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetric2
{
private:
    double MinValue;
    double MaxValue;
    int BucketCount;

    int NumValues;
    int Sum;
    int SumSquares;

    int Delta;

    //! Contains values from (-oo, MinValue).
    int MinimalBucket;

    //! Contains values from (MaxValue, oo).
    int MaximalBucket;

    //! Contains values fro [MinValue, MaxValue]
    yvector<int> Buckets;


public:
    TMetric2(double minValue, double maxValue, int bucketCount);

    //! Adds value to analyzis.
    void AddValue(double value);

    //! Returns mean of all values.
    double GetMean() const;

    //! Returns standart deviation of all values.
    double GetStdDev() const;

    //! Returns information about all values.
    Stroka GetDebugInfo() const;

private:
    //! Puts the #value to appropriate bucket.
    void UpdateBuckets(double value);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
