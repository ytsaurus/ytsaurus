#include "config_wrapper.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulingPolicyConfigWrapper::TGpuSchedulingPolicyConfigWrapper(TGpuSchedulingPolicyConfigPtr config)
    : Config_(std::move(config))
    , Modules_(GetKeySet(Config_->ModuleConfigs))
{ }

const TGpuSchedulingPolicyConfigPtr& TGpuSchedulingPolicyConfigWrapper::GetConfig() const
{
    return Config_;
}

const TGpuSchedulingPolicyConfig* TGpuSchedulingPolicyConfigWrapper::operator->() const
{
    return Config_.Get();
}

const THashSet<std::string>& TGpuSchedulingPolicyConfigWrapper::GetModules() const
{
    return Modules_;
}

TDuration TGpuSchedulingPolicyConfigWrapper::GetModuleReconsiderationTimeout(const std::string& module) const
{
    const auto* moduleConfig = FindModuleConfig(module);
    if (moduleConfig && moduleConfig->ModuleReconsiderationTimeout) {
        return *moduleConfig->ModuleReconsiderationTimeout;
    }
    return Config_->ModuleReconsiderationTimeout;
}

const std::vector<TModuleShareAndNetworkPriority>& TGpuSchedulingPolicyConfigWrapper::GetModuleShareToNetworkPriority(
    const std::string& module) const
{
    const auto* moduleConfig = FindModuleConfig(module);
    if (moduleConfig && moduleConfig->ModuleShareToNetworkPriority) {
        return *moduleConfig->ModuleShareToNetworkPriority;
    }
    return Config_->ModuleShareToNetworkPriority;
}

// TODO(eshcherbin): Use GetOrCrash here once operations bound to a module removed
// from the config are handled gracefully during module states initialization.
const TGpuSchedulingPolicyModuleConfig* TGpuSchedulingPolicyConfigWrapper::FindModuleConfig(const std::string& module) const
{
    const auto* moduleConfig = Config_->ModuleConfigs.FindPtr(module);
    return moduleConfig ? moduleConfig->Get() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
