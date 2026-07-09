#pragma once

#include "public.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// Implements an adaptive row-count limit for paginated SelectRows calls.
// Starts at SelectMinLimit, multiplies by SelectLimitMultiplier on each call,
// and caps at SelectMaxLimit. Used to amortize per-request overhead when
// iterating over large ranges in a loop.
class TSelectLimiter
{
public:
    explicit TSelectLimiter(TDynamicTableRequestSpecPtr spec);

    i64 Get();

private:
    TDynamicTableRequestSpecPtr Spec_;
    i64 Current_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
