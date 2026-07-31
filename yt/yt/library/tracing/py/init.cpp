#include "init.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/tracing/jaeger/config.h>
#include <yt/yt/library/tracing/jaeger/tracer.h>

namespace NYT::NTracing {

using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

void InitializeGlobalTracer(const std::string& config)
{
    auto configPtr = ConvertTo<TJaegerTracerConfigPtr>(TYsonString{config});
    SetGlobalTracer(New<TJaegerTracer>(configPtr));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
