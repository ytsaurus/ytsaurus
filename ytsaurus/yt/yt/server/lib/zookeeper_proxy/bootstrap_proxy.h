#pragma once

#include "public.h"

namespace NYT::NZookeeperProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrapProxy
{
    virtual ~IBootstrapProxy() = default;

    //! Returns proxy config.
    virtual const TZookeeperProxyConfigPtr& GetConfig() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
