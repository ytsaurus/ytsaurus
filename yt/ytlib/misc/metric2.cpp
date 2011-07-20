#include "metric2.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMetric2::TMetric2(double minValue, double maxValue, int bucketCount)
    : MinValue(minValue)
    , MaxValue(maxValue)
    , BucketCount(bucketCount)
    , Buckets(bucketCount)
    , NumValues(0)
    , Sum(0)
    , SumSquares(0)
{
    Delta = (MaxValue - MinValue) / BucketCount;
}

void TMetric2::AddValue(double value)
{
    Sum += value;
    SumSquares += value * value;
    NumValues += 1;

    UpdateBuckets(value);
}

double TMetric2::GetMean() const
{
    if (NumValues == 0) {
        return 0;
    }
    return Sum / NumValues;
}

double TMetric2::GetStdDev() const
{
    if (NumValues == 0) {
        return 0;
    }
    double mean = GetMean();
    return sqrt(abs(SumSquares / NumValues - mean * mean));
}

Stroka TMetric2::GetDebugInfo() const
{
    // TODO: implement
    return "";
}

void TMetric2::UpdateBuckets(double value)
{
    if (value < MinValue) {
        MinimalBucket += 1;
    } else if (value > MaxValue) {
        MaximalBucket += 1;
    } else {
        int BucketId = (value - MinValue) / Delta;

        // in case of roundings errors
        if (BucketId < 0) BucketId = 0;
        if (BucketId >= BucketCount) BucketId = BucketCount - 1;

        Buckets[BucketId] += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
