#include "mutation.h"

#include <yt/core/rpc/service.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TMutation::TMutation(IHydraManagerPtr hydraManager)
    : HydraManager_(std::move(hydraManager))
{ }

TFuture<TMutationResponse> TMutation::Commit()
{
    return HydraManager_->CommitMutation(std::move(Request_));
}

TFuture<TMutationResponse> TMutation::CommitAndLog(const NLogging::TLogger& logger)
{
    auto type = Request_.Type;
    return Commit().Apply(
        BIND([Logger = logger, type = std::move(type)] (const TErrorOr<TMutationResponse>& result) {
            if (result.IsOK()) {
                YT_LOG_DEBUG("Mutation commit succeeded (MutationType: %v)", type);
            } else {
                YT_LOG_DEBUG(result, "Mutation commit failed (MutationType: %v)", type);
            }
            return result;
        }));
}

TFuture<TMutationResponse> TMutation::CommitAndReply(NRpc::IServiceContextPtr context)
{
    return Commit().Apply(
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
        }));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
