#include "mutation.h"

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TMutation::TMutation(ISimpleHydraManagerPtr hydraManager)
    : HydraManager_(std::move(hydraManager))
{
    Request_.Reign = HydraManager_->GetCurrentReign();
    Request_.TractoReign = HydraManager_->GetCurrentTractoReign();
}

TFuture<TMutationResponse> TMutation::Commit()
{
    return HydraManager_->CommitMutation(std::move(Request_));
}

TFuture<TMutationResponse> TMutation::CommitAndLog(NLogging::TLogger logger)
{
    // NB: We need to capture the type before calling Commit as the latter will move the request.
    auto callback =
        BIND([Logger = std::move(logger), type = Request_.Type] (const TErrorOr<TMutationResponse>& result) {
            if (result.IsOK()) {
                YT_LOG_DEBUG("Mutation commit succeeded (MutationType: %v)", type);
            } else {
                YT_LOG_DEBUG(result, "Mutation commit failed (MutationType: %v)", type);
            }
            return result;
        });
    return Commit().Apply(std::move(callback));
}

TFuture<TMutationResponse> TMutation::CommitAndReply(NRpc::IServiceContextPtr context)
{
    // NB: Just for the symmetry with CommitAndLog.
    auto callback =
        BIND([context = std::move(context)] (const TErrorOr<TMutationResponse>& result) {
            if (!context->IsReplied()) {
                if (result.IsOK()) {
                    const auto& response = result.Value();
                    if (response.Data) {
                        context->Reply(response.Data);
                    } else {
                        context->Reply(TError());
                    }
                } else {
                    context->Reply(TError(result));
                }
            }
            return result;
        });
    return Commit().Apply(std::move(callback));
}

void TMutation::SetRequestData(TSharedRef data, TString type)
{
    Request_.Data = std::move(data);
    Request_.Type = std::move(type);
}

void TMutation::SetHandler(TCallback<void(TMutationContext*)> handler)
{
    Request_.Handler = std::move(handler);
}

void TMutation::SetAllowLeaderForwarding(bool value)
{
    Request_.AllowLeaderForwarding = value;
}

void TMutation::SetMutationId(NRpc::TMutationId mutationId, bool retry)
{
    Request_.MutationId = mutationId;
    Request_.Retry = retry;
}

void TMutation::SetEpochId(TEpochId epochId)
{
    Request_.EpochId = epochId;
}

void TMutation::SetTraceContext(NTracing::TTraceContextPtr traceContext)
{
    if (traceContext && traceContext->IsRecorded()) {
        traceContext = traceContext->CreateChild(ConcatToString(TStringBuf("HydraMutation:"), Request_.Type));

        if (Request_.MutationId) {
            traceContext->AddTag("mutation_id", ToString(Request_.MutationId));
        }
    }

    Request_.TraceContext = std::move(traceContext);
}

void TMutation::SetCurrentTraceContext()
{
    SetTraceContext(NTracing::TryGetCurrentTraceContext());
}

const TString& TMutation::GetType() const
{
    return Request_.Type;
}

const TSharedRef& TMutation::GetData() const
{
    return Request_.Data;
}

NRpc::TMutationId TMutation::GetMutationId() const
{
    return Request_.MutationId;
}

bool TMutation::IsRetry() const
{
    return Request_.Retry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
