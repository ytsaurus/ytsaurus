#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Run() = 0;

    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const NApi::NNative::IClientPtr& GetClient() const = 0;
    virtual const NCypressElection::ICypressElectionManagerPtr& GetElectionManager() const = 0;
    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TTabletBalancerServerConfigPtr config, NYTree::INodePtr configNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
