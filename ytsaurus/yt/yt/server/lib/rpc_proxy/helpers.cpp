#include "helpers.h"

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NRpcProxy {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

void PutMethodInfoInTraceContext(const TStringBuf& methodName)
{
    if (auto* traceContext = TryGetCurrentTraceContext()) {
        auto baggage = traceContext->UnpackOrCreateBaggage();
        AddTagToBaggage(baggage, EAggregateIOTag::ApiMethod, methodName);
        AddTagToBaggage(baggage, EAggregateIOTag::ProxyKind, "rpc");
        traceContext->PackBaggage(baggage);
    }
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
