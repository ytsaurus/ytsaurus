#include "private.h"

#include <library/cpp/yt/string/guid.h>

namespace NYT::NControllerAgent {

using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TJobMonitoringDescriptor& descriptor, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "%v/%v",
        descriptor.IncarnationId,
        descriptor.Index);
}

TString ToString(const TJobMonitoringDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

TTraceContextGuard CreateOperationTraceContextGuard(
    TString spanName,
    TOperationId operationId)
{
    auto traceContext = CreateTraceContextFromCurrent(std::move(spanName));
    traceContext->SetAllocationTags({{OperationIdTag, ToString(operationId)}});
    return TTraceContextGuard(std::move(traceContext));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
