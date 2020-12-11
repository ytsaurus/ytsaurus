#pragma once

#include "public.h"

#include <yt/server/lib/dynamic_config/dynamic_config_manager.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of YT node
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: Control (unless noted otherwise)
 */
class TClusterNodeDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TClusterNodeDynamicConfig>
{
public:
    TClusterNodeDynamicConfigManager(const TBootstrap* bootstrap);

protected:
    virtual std::vector<TString> GetInstanceTags() const override;

private:
    const TBootstrap* Bootstrap_;
};

DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
