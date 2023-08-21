#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const NApi::NNative::IClientPtr& GetClient() = 0;
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual NNodeTrackerClient::TAddressMap GetLocalAddresses() const = 0;
    virtual const NCypressElection::ICypressElectionManagerPtr& GetElectionManager() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TCellBalancerBootstrapConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
