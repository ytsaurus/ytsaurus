#include "companion_computation_base.h"

#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

TCompanionResponsePtr ProcessWithCompanionHealing(
    const ICompanionClientPtr& client,
    const TCompanionProcessRequestPtr& request,
    const IExternalPerformanceMetricsReporterPtr& reporter,
    const std::function<std::vector<TCompanionResourceInstanceReference>()>& healRequiredCompanionResources)
{
    // After a companion restart JobNotFound and ResourceNotInitialized can occur
    // back to back, hence more than two attempts.
    constexpr int MaxAttempts = 3;

    auto response = client->DoProcessWithCompanionSync(request, reporter);
    for (int attempt = 1; attempt < MaxAttempts; ++attempt) {
        if (response->Status == ECompanionResponseStatus::JobNotFound) {
            // Resend the request with job info included.
            request->SendJobInfo = true;
        } else if (response->Status == ECompanionResponseStatus::ResourceNotInitialized) {
            request->CompanionResources = healRequiredCompanionResources();
            // Recreate the cached companion job so it acquires the exact
            // resource instances that have just been initialized.
            request->SendJobInfo = true;
        } else {
            break;
        }
        response = client->DoProcessWithCompanionSync(request, reporter);
    }
    return response;
}

////////////////////////////////////////////////////////////////////////////////

void AddJoinedExternalStates(
    const TCompanionProcessRequestPtr& request,
    const THashMap<std::string, TJoinedStateKeyClient<TSimpleExternalState>>& joiners,
    const IInputContextPtr& input)
{
    for (const auto& [stateName, stateClient] : joiners) {
        for (const auto& key : stateClient.ExtractKeys(input)) {
            auto stateHandle = stateClient.GetState(key);
            const auto* joinedState = stateHandle.Get();
            if (!joinedState) {
                continue;
            }
            GetOrInsert(
                request->JoinedExternalStates,
                stateName,
                [&] {
                    return TStateHolder<TPayload>{
                        .StateName = stateName,
                        .Schema = joinedState->Schema,
                    };
                })
                .StateItems.push_back({
                    .Key = key,
                    .State = joinedState->Payload,
                });
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
