#pragma once

#include "public.h"

#include "value.h"

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

struct IRangeFilter
{
    virtual ~IRangeFilter() = default;

    /// Returns True if the filter condition is feasible in the given key range.
    virtual bool CheckRange(
        const TValue* leftKey,
        const TValue* rightKey,
        size_t keySize) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
