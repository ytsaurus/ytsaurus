#pragma once

#include <core/misc/public.h>

namespace NYT {
namespace NTracing {

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTraceManagerConfig)

class TTraceContext;

typedef ui64 TTraceId;
const TTraceId InvalidTraceId = 0;

typedef ui64 TSpanId;
const TSpanId InvalidSpanId = 0;

///////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT
