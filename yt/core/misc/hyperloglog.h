#pragma once

#include "public.h"

namespace NYT {

class HyperLogLog
{
public:
    HyperLogLog(int precision);

    void Add(ui64 value);

    void Merge(HyperLogLog that);

    ui64 EstimateCardinality();

private:
    int Precision_;
    ui64 RegisterCount_;
    ui64 PrecisionMask_;
    std::vector<long> ZeroCounts_;
};

} // namespace NYT
