#include "native_authentication_manager.h"

#include "config.h"

namespace NYT::NAuth {

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

    auto appliedConfig = CloneYsonSerializable(config);
    appliedConfig->ClientEnableServiceTicketFetching = true;
    YT_VERIFY(appliedConfig->ClientDstMap.emplace("self", appliedConfig->ClientSelfId).second);
    return CreateDynamicTvmService(appliedConfig);
}

void TNativeAuthenticationManager::Configure(const TNativeAuthenticationManagerConfigPtr& config)
{
    TvmService_.Store(CreateTvmService(config->TvmService));
    EnableValidation_.store(config->EnableValidation);
}

void TNativeAuthenticationManager::Reconfigure(const TNativeAuthenticationManagerDynamicConfigPtr& config)
{
    if (config->EnableValidation) {
        EnableValidation_.store(*config->EnableValidation);
    }
}

IDynamicTvmServicePtr TNativeAuthenticationManager::GetTvmService() const
{
    return TvmService_.Load();
}

bool TNativeAuthenticationManager::IsValidationEnabled() const
{
    return EnableValidation_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
