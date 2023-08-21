#pragma once

#include "public.h"

#include "bootstrap_proxy.h"

namespace NYT::NZookeeperMaster {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public IBootstrapProxy
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;

    virtual const IZookeeperManagerPtr& GetZookeeperManager() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(IBootstrapProxy* bootstrapProxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
