#pragma once

#include "trace_context.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

// Used in fibers trace context printer                                                         
// devtools/gdb/yt_fibers_printer.py: find_trace_context()                                      
TTraceContext* RetrieveTraceContextFromPropStorage(NConcurrency::TPropagatingStorage* storage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
