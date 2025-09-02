#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{
    virtual const TChaosCacheBootstrapConfigPtr& GetConfig() const = 0;
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const NApi::NNative::IClientPtr& GetRootClient() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateChaosCacheBootstrap(
    TChaosCacheBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
