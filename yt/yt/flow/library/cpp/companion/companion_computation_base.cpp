#include "companion_computation_base.h"

#include <yt/yt/flow/library/cpp/common/companion_state_adapter.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TCompanionFunctionParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("function_ids", &TThis::FunctionIds)
        .Default();

    registrar.Postprocessor([] (TThis* parameters) {
        if (!parameters->FunctionIds) {
            return;
        }
        THROW_ERROR_EXCEPTION_IF(parameters->FunctionIds->empty(), "\"function_ids\" cannot be empty");
        THashSet<std::string> functions;
        for (const auto& function : *parameters->FunctionIds) {
            THROW_ERROR_EXCEPTION_UNLESS(
                functions.insert(function).second,
                "\"function_ids\" lists function %Qv more than once",
                function);
        }
    });
}

void ValidateCompanionFunctionIds(
    const TCompanionFunctionParameters& parameters,
    const TCompanionComputationInfo& computationInfo)
{
    // A companion that does not resolve the IDs would silently run only the function of the computation ID.
    THROW_ERROR_EXCEPTION_UNLESS(
        !parameters.FunctionIds || computationInfo.SupportsFunctionIds,
        "Companion does not support \"function_ids\"; update the companion SDK or remove the parameter")
        .With("computation_id", computationInfo.ComputationId)
        .With("function_ids", *parameters.FunctionIds);
}

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
    const THashMap<std::string, ICompanionStateAdapterPtr>& joiners,
    const IInputContextPtr& input)
{
    for (const auto& [stateName, adapter] : joiners) {
        for (const auto& key : adapter->ExtractKeys(input)) {
            auto payload = adapter->EncodeState(key);
            if (!payload) {
                continue;
            }
            GetOrInsert(
                request->JoinedExternalStates,
                stateName,
                [&] {
                    auto descriptor = adapter->Describe();
                    return TStateHolder<TSharedRef>{
                        .StateName = stateName,
                        .Schema = descriptor.Schema,
                        .Format = descriptor.Format,
                        .ProtoType = descriptor.ProtoType,
                    };
                })
                .StateItems.push_back({
                    .Key = key,
                    .State = std::move(payload),
                });
        }
    }
}

void ValidateExternalStateManagersAutoPreload(const TComputationSpec& spec)
{
    for (const auto& [name, managerSpec] : spec.ExternalStateManagers) {
        THROW_ERROR_EXCEPTION_IF(!managerSpec->AutoPreload,
            "External state manager %Qv has auto_preload disabled, which a companion computation "
            "cannot honor: companion states are shipped with the batch and cannot be preloaded on demand",
            name);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
