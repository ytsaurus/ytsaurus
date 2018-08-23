#pragma once

#include "value.h"

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

class IRangeFilter
{
public:
    virtual ~IRangeFilter() = default;

    /// Returns True if the filter condition is feasible in the given key range.
    virtual bool CheckRange(
        const TValue* leftKey,
        const TValue* rightKey,
        size_t keySize) const = 0;
};

using IRangeFilterPtr = std::shared_ptr<IRangeFilter>;

}   // namespace NInterop
