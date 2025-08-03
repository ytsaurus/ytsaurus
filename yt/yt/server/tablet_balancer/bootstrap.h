#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_election/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const NApi::NNative::IClientPtr& GetClient() const = 0;
    virtual const NHiveClient::TClientDirectoryPtr& GetClientDirectory() const = 0;
    virtual const NCypressElection::ICypressElectionManagerPtr& GetElectionManager() const = 0;
    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;
    virtual std::string GetClusterName() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateTabletBalancerBootstrap(
    TTabletBalancerBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
