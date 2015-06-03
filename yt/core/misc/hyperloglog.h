#pragma once

#include "public.h"
#include <core/misc/farm_hash.h>

namespace NYT {

template <int precision>
class HyperLogLog
{
public:
    HyperLogLog();

    void Add(ui64 value);

    void Add(char* data, size_t length);

    void Merge(HyperLogLog* that);

    ui64 EstimateCardinality();

private:
    ui64 RegisterCount_;
    ui64 PrecisionMask_;
    long ZeroCounts_[1 << precision];

    void AddHash(ui64 hash);
};

template<int precision>
long EstimateCardinality(
    std::vector<long> values)
{
    auto state = HyperLogLog<precision>();
    for (auto v : values) {
        state.Add(v);
    }
    return state.EstimateCardinality();
}

template<int precision>
HyperLogLog<precision>::HyperLogLog()
    : RegisterCount_((ui64)1 << precision)
    , PrecisionMask_(RegisterCount_ - 1)
{
    for (auto& count : ZeroCounts_) {
        count = 0;
    }
}

template<int precision>
void HyperLogLog<precision>::AddHash(ui64 hash)
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

template<int precision>
void HyperLogLog<precision>::Add(ui64 value)
{
    AddHash(FarmHash(value));
}

template<int precision>
void HyperLogLog<precision>::Add(char* data, size_t length)
{
    AddHash(FarmHash(data, length));
}

template<int precision>
void HyperLogLog<precision>::Merge(HyperLogLog<precision>* that)
{
    for (int i = 0; i < RegisterCount_; i++) {
        auto thatCount = that->ZeroCounts_[i];
        if (ZeroCounts_[i] < thatCount) {
            ZeroCounts_[i] = thatCount;
        }
    }
}

template<int precision>
ui64 HyperLogLog<precision>::EstimateCardinality()
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
