#pragma once

#include "public.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of YT node
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TClusterNodeDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TClusterNodeDynamicConfig>
{
public:
    explicit TClusterNodeDynamicConfigManager(IBootstrap* bootstrap);

    explicit TClusterNodeDynamicConfigManager(TClusterNodeDynamicConfigPtr staticConfig);

    //! Starts the dynamic config manager.
    void Start();

protected:
    std::vector<TString> GetInstanceTags() const override;

private:
    IBootstrap* const Bootstrap_;
};

DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
