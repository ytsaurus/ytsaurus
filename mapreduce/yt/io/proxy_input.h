#pragma once

#include <mapreduce/yt/interface/io.h>

class yexception;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProxyInput
    : public TRawTableReader
{
public:
    virtual bool OnStreamError(
        const yexception& e,
        bool keepRanges,
        ui32 rangeIndex,
        ui64 rowIndex) = 0;

    virtual bool HasRangeIndices() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
