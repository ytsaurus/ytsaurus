#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

class TBundleDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TBundleDynamicConfig>
{
public:
    explicit TBundleDynamicConfigManager(NClusterNode::IBootstrap* bootstrap);

    explicit TBundleDynamicConfigManager(TBundleDynamicConfigPtr staticConfig);

    void Start();

protected:
    std::vector<TString> GetInstanceTags() const override;

private:
    NClusterNode::IBootstrap* const Bootstrap_;
};

DECLARE_REFCOUNTED_CLASS(TBundleDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
