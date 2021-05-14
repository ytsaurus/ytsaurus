#pragma once

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/guid.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTracingExt;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TTraceContext)

using TTraceId = TGuid;
constexpr TTraceId InvalidTraceId = {};

using TSpanId = ui64;
constexpr TSpanId InvalidSpanId = 0;

// Request ids come from RPC infrastructure but
// we should avoid include-dependencies here.
using TRequestId = TGuid;
constexpr TRequestId InvalidRequestId = {};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
