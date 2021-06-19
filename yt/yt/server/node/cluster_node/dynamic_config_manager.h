#pragma once

#include "public.h"

#include <yt/yt/server/lib/dynamic_config/dynamic_config_manager.h>

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
    explicit TClusterNodeDynamicConfigManager(TBootstrap* bootstrap);

    //! Starts the dynamic config manager.
    void Start();

protected:
    virtual std::vector<TString> GetInstanceTags() const override;

private:
    TBootstrap* const Bootstrap_;
};

DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
