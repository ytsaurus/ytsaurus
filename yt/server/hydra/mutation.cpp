#include "stdafx.h"
#include "mutation.h"

#include <core/concurrency/delayed_executor.h>

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

////////////////////////////////////////////////////////////////////////////////

TMutationPtr CreateMutation(IHydraManagerPtr hydraManager)
{
    return New<TMutation>(std::move(hydraManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
