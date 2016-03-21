#pragma once

#include <util/stream/input.h>

class yexception;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TProxyInput
    : public TInputStream
{
public:
    virtual bool OnStreamError(const yexception& e, ui32 rangeIndex, ui64 rowIndex) = 0;
    virtual bool HasRangeIndices() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
