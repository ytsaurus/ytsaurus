#pragma once

#include <yt/yt/flow/library/cpp/common/seq_no_provider.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TMonotonicSeqNoProvider
    : public ISeqNoProvider
{
public:
    i64 Generate() override
    {
        return Counter_++;
    }

    i64 GenerateAlignedBatch(i64 count, i64 alignment) override
    {
        if (auto rem = Counter_ % alignment; rem != 0) {
            Counter_ += alignment - rem;
        }
        auto base = Counter_;
        Counter_ += count;
        return base;
    }

private:
    i64 Counter_ = 1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
