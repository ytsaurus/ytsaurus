#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{
    virtual const NApi::NNative::IClientPtr& GetClient() = 0;
    virtual const IInvokerPtr& GetControlInvoker() const = 0;

    virtual NNodeTrackerClient::TAddressMap GetLocalAddresses() const = 0;
    virtual const NCypressElection::ICypressElectionManagerPtr& GetElectionManager() const = 0;

    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateCellBalancerBootstrap(
    TCellBalancerBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
