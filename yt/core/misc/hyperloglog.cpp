#include "hyperloglog.h"

#include <core/misc/farm_hash.h>

namespace NYT {

long EstimateCardinality(
    std::vector<long> values,
    int precision)
{
    auto state = HyperLogLog(precision);
    for (auto v : values) {
        state.Add(v);
    }
    return state.EstimateCardinality();
}

HyperLogLog::HyperLogLog(int precision)
    : Precision_(precision)
    , RegisterCount_((ui64)1 << precision)
    , PrecisionMask_(RegisterCount_ - 1)
    , ZeroCounts_(std::vector<long>(RegisterCount_))
{ }

void HyperLogLog::AddHash(ui64 hash)
{
    auto zeroes = 1;
    hash |= ((ui64)1 << 63);
    auto bit = RegisterCount_;

    while ((bit & hash) == 0) {
        zeroes++;
        bit <<= 1;
    }

    auto index = hash & PrecisionMask_;
    if (ZeroCounts_[index] < zeroes) {
        ZeroCounts_[index] = zeroes;
    }
}

void HyperLogLog::Add(ui64 value)
{
    AddHash(FarmHash(value));
}

void HyperLogLog::Add(char* data, size_t length)
{
    AddHash(FarmHash(data, length));
}

void HyperLogLog::Merge(HyperLogLog that)
{
    YCHECK(that.Precision_ == Precision_);

    auto thatCount = that.ZeroCounts_.begin();
    for (auto& count : ZeroCounts_) {
        if (count < *thatCount) {
            count = *thatCount;
        }
        thatCount++;
    }
}

ui64 HyperLogLog::EstimateCardinality()
{
    auto zeroRegisters = 0;
    double sum = 0;
    for (auto count : ZeroCounts_) {
        if (count == 0) {
            zeroRegisters++;
        } else {
            sum += 1.0/((ui64)1 << count);
        }
    }
    sum += zeroRegisters;

    double alpha = 0.7213/(1+1.079/RegisterCount_);
    double m = RegisterCount_;
    double raw = (1.0/sum) * m * m * alpha;

    if (raw < 2.5 * m && zeroRegisters != 0) {
        return m * log(m / zeroRegisters);
    } else {
        return raw;
    }
}

} // namespace NYT
