#include "native_authentication_manager.h"

#include "config.h"
#include "private.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NativeAuthLogger;

////////////////////////////////////////////////////////////////////////////////

TNativeAuthenticationManager* TNativeAuthenticationManager::Get()
{
    return Singleton<TNativeAuthenticationManager>();
}

IDynamicTvmServicePtr TNativeAuthenticationManager::CreateTvmService(const TTvmServiceConfigPtr& config)
{
    if (!config) {
        return nullptr;
    }

    auto appliedConfig = CloneYsonStruct(config);
    appliedConfig->ClientEnableServiceTicketFetching = true;
    YT_VERIFY(appliedConfig->ClientDstMap.emplace("self", appliedConfig->ClientSelfId).second);
    return CreateDynamicTvmService(appliedConfig, AuthProfiler.WithPrefix("/native_tvm"));
}

void TNativeAuthenticationManager::Configure(const TNativeAuthenticationManagerConfigPtr& config)
{
    TvmService_.Store(CreateTvmService(config->TvmService));
    EnableValidation_.store(config->EnableValidation);
    EnableSubmission_.store(config->EnableSubmission);
}

void TNativeAuthenticationManager::Reconfigure(const TNativeAuthenticationManagerDynamicConfigPtr& config)
{
    if (config->EnableValidation) {
        EnableValidation_.store(*config->EnableValidation);
    }
    if (config->EnableSubmission) {
        EnableSubmission_.store(*config->EnableSubmission);
    }
    if (EnableValidation_.load() && !EnableSubmission_.load()) {
        YT_LOG_WARNING("Disabling ticket validation automatically when submission is disabled");
        EnableValidation_.store(false);
    }
}

IDynamicTvmServicePtr TNativeAuthenticationManager::GetTvmService() const
{
    return TvmService_.Load();
}

void TNativeAuthenticationManager::SetTvmService(IDynamicTvmServicePtr tvmService)
{
    TvmService_.Store(std::move(tvmService));
}

bool TNativeAuthenticationManager::IsValidationEnabled() const
{
    return EnableValidation_.load(std::memory_order::relaxed);
}

bool TNativeAuthenticationManager::IsSubmissionEnabled() const
{
    return EnableSubmission_.load(std::memory_order::relaxed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
