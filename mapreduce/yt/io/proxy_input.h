#pragma once

#include <mapreduce/yt/interface/io.h>

class yexception;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProxyInput
    : public TRawTableReader
{
public:
    virtual bool Retry(
        const TMaybe<ui32>& /*rangeIndex*/,
        const TMaybe<ui64>& /*rowIndex*/) override
    {
        return false;
    }

    virtual bool HasRangeIndices() const override {
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
