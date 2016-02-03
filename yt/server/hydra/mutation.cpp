#include "mutation.h"

#include <yt/core/rpc/service.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TMutation::TMutation(IHydraManagerPtr hydraManager)
    : HydraManager_(std::move(hydraManager))
{ }

TFuture<TMutationResponse> TMutation::Commit()
{
    return HydraManager_->CommitMutation(Request_);
}

TFuture<TMutationResponse> TMutation::CommitAndLog(const NLogging::TLogger& logger)
{
    auto type = Request_.Type;
    return Commit().Apply(BIND([=] (const TErrorOr<TMutationResponse>& result) {
        const auto& Logger = logger;
        if (result.IsOK()) {
            LOG_INFO("Mutation commit succeeded (MutationType: %v)", type);
            return result.Value();
        } else {
            LOG_ERROR(result, "Mutation commit failed (MutationType: %v)", type);
            THROW_ERROR result;
        }
    }));
}

TFuture<TMutationResponse> TMutation::CommitAndReply(NRpc::IServiceContextPtr context)
{
    return Commit().Apply(BIND([=] (const TErrorOr<TMutationResponse>& result) {
        if (result.IsOK()) {
            if (!context->IsReplied()) {
                const auto& response = result.Value();
                if (response.Data) {
                    context->Reply(response.Data);
                } else {
                    context->Reply(TError());
                }
            }
            return result.Value();
        } else {
            if (!context->IsReplied()) {
                context->Reply(result);
            }
            THROW_ERROR result;
        }
    }));
}

TMutationPtr TMutation::SetRequestData(TSharedRef data, Stroka type)
{
    Request_.Data = std::move(data);
    Request_.Type = std::move(type);
    return this;
}

TMutationPtr TMutation::SetAction(TCallback<void(TMutationContext*)> action)
{
    Request_.Action = std::move(action);
    return this;
}

TMutationPtr TMutation::SetAllowLeaderForwarding(bool value)
{
    Request_.AllowLeaderForwarding = value;
    return this;
}

////////////////////////////////////////////////////////////////////////////////

TMutationPtr CreateMutation(IHydraManagerPtr hydraManager)
{
    return New<TMutation>(std::move(hydraManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
