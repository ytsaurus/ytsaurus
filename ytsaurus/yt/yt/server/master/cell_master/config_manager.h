#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct IConfigManager
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual const TDynamicClusterConfigPtr& GetConfig() const = 0;
    virtual void SetConfig(NYTree::INodePtr configNode) = 0;

    DECLARE_INTERFACE_SIGNAL(void(TDynamicClusterConfigPtr), ConfigChanged);
};

DEFINE_REFCOUNTED_TYPE(IConfigManager)

////////////////////////////////////////////////////////////////////////////////

IConfigManagerPtr CreateConfigManager(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
