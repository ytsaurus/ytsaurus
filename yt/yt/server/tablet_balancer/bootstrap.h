#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;
    virtual void Run() = 0;

    virtual const NApi::NNative::IClientPtr& GetMasterClient() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TTabletBalancerServerConfigPtr config, NYTree::INodePtr configNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
