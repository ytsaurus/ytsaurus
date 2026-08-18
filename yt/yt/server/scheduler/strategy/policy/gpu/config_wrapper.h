#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

//! Wraps TGpuSchedulingPolicyConfig, precomputing the effective module set
//! and providing per-module option getters.
class TGpuSchedulingPolicyConfigWrapper
{
public:
    explicit TGpuSchedulingPolicyConfigWrapper(TGpuSchedulingPolicyConfigPtr config);

    const TGpuSchedulingPolicyConfigPtr& GetConfig() const;
    const TGpuSchedulingPolicyConfig* operator->() const;

    //! Effective module set: keys of "module_configs".
    const THashSet<std::string>& GetModules() const;

    //! Per-module getters fall back to the tree-level values for unknown modules
    //! (an operation may still be bound to a module recently removed from the config).
    TDuration GetModuleReconsiderationTimeout(const std::string& module) const;

    const std::vector<TModuleShareAndNetworkPriority>& GetModuleShareToNetworkPriority(
        const std::string& module) const;

private:
    TGpuSchedulingPolicyConfigPtr Config_;
    THashSet<std::string> Modules_;

    const TGpuSchedulingPolicyModuleConfig* FindModuleConfig(const std::string& module) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
