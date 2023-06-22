#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

//! Used in fibers trace context printer.
//! See devtools/gdb/yt_fibers_printer.py.
TTraceContext* RetrieveTraceContextFromPropStorage(NConcurrency::TPropagatingStorage* storage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
