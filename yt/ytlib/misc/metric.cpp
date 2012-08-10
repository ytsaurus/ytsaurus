#include "stdafx.h"
#include "metric.h"

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMetric::TMetric(double minValue, double maxValue, int bucketCount)
    : MinValue(minValue)
    , MaxValue(maxValue)
    , BucketCount(bucketCount)
    , ValueCount(0)
    , Sum(0)
    , SumSquares(0)
    , MinBucket(0)
    , MaxBucket(0)
    , Buckets(bucketCount)
{
    Delta = (MaxValue - MinValue) / BucketCount;
}

void TMetric::AddValue(double value)
{
    Sum += value;
    SumSquares += value * value;
    ValueCount++;

    IncrementBucket(value);
}

void TMetric::AddValue(TDuration duration)
{
    AddValue(static_cast<double>(duration.MilliSeconds()));
}

void TMetric::AddDelta(TInstant start)
{
    AddValue(TInstant::Now() - start);
}

double TMetric::GetMean() const
{
    if (ValueCount == 0) {
        return 0;
    }
    return Sum / ValueCount;
}

double TMetric::GetStd() const
{
    if (ValueCount == 0) {
        return 0;
    }
    double mean = GetMean();
    return std::sqrt(std::fabs(SumSquares / ValueCount - mean * mean));
}

Stroka TMetric::GetDebugInfo() const
{
    Stroka info = Sprintf("Count: %d, Mean: %lf, Std: %lf",
        ValueCount, GetMean(), GetStd());
//    info += Sprintf("%d values in (-inf, %lf)\n", MinBucket, MinValue);
//    double left = MinValue;
//    double right = left + Delta;
//    for (int i = 0; i < Buckets.size(); ++i) {
//        if (Buckets[i] == 0) continue;
//        info += Sprintf("%d values in (%lf, %lf)\n", Buckets[i], left, right);
//        left += Delta;
//        right += Delta;
//    }
//    info += Sprintf("%d values in (%lf, +inf)\n", MaxBucket, MaxValue);
    return info;
}

void TMetric::IncrementBucket(double value)
{
    if (value < MinValue) {
        MinBucket += 1;
    } else if (value >= MaxValue) {
        MaxBucket += 1;
    } else {
        int BucketId = static_cast<int>((value - MinValue) / Delta);

        // in case of roundings errors
        if (BucketId < 0) BucketId = 0;
        if (BucketId >= BucketCount) BucketId = BucketCount - 1;

        Buckets[BucketId] += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
