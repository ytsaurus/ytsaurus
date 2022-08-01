#pragma once

#include "public.h"

#include <yt/yt/server/lib/dynamic_config/dynamic_config_manager.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

class TBundleDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TBundleDynamicConfig>
{
public:
    explicit TBundleDynamicConfigManager(IBootstrap* bootstrap);

    explicit TBundleDynamicConfigManager(TBundleDynamicConfigPtr staticConfig);

    void Start();

protected:
    std::vector<TString> GetInstanceTags() const override;

private:
    IBootstrap* const Bootstrap_;
};

DECLARE_REFCOUNTED_CLASS(TBundleDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
