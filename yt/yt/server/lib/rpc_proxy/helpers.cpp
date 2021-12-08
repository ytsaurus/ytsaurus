#include "helpers.h"

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NRpcProxy {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

void PutMethodInfoInTraceContext(const TStringBuf& methodName)
{
    if (auto traceContext = GetCurrentTraceContext()) {
        auto baggage = traceContext->UnpackOrCreateBaggage();
        baggage->Set("api_method@", methodName);
        baggage->Set("proxy_type@", "rpc");
        traceContext->PackBaggage(baggage);
    }
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
