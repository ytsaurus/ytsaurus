#include "mutation.h"

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/common.h>

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
