#include "batch_trace.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

void TBatchTrace::Join()
{
    if (auto context = GetCurrentTraceContext(); context && context->IsRecorded()) {
        Join(MakeStrong(context));
    }
}

void TBatchTrace::Join(const TTraceContextPtr& context)
{
    if (!context->IsRecorded()) {
        return;
    }

    Clients_.push_back(context);
}

std::pair<TTraceContextPtr, bool> TBatchTrace::StartSpan(const TString& spanName)
{
    auto traceContext = TTraceContext::NewRoot(spanName);
    if (Clients_.empty()) {
        return {traceContext, false};
    }

    traceContext->SetSampled();
    for (const auto& client : Clients_) {
        client->AddAsyncChild(traceContext->GetTraceId());
    }
    Clients_.clear();

    return {traceContext, true};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
