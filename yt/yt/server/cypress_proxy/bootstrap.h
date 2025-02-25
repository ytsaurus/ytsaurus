#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public NServer::IDaemonBootstrap
{
    virtual const TCypressProxyBootstrapConfigPtr& GetConfig() const = 0;

    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;

    virtual const TUserDirectoryPtr& GetUserDirectory() const = 0;
    virtual const IUserDirectorySynchronizerPtr& GetUserDirectorySynchronizer() const = 0;

    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;

    virtual const IInvokerPtr& GetControlInvoker() const = 0;

    virtual const NApi::NNative::IConnectionPtr& GetNativeConnection() const = 0;
    virtual const NApi::NNative::IClientPtr& GetNativeRootClient() const = 0;

    virtual const NSequoiaClient::ISequoiaClientPtr& GetSequoiaClient() const = 0;

    virtual NApi::IClientPtr GetRootClient() const = 0;

    virtual const ISequoiaServicePtr& GetSequoiaService() const = 0;

    virtual const ISequoiaResponseKeeperPtr& GetResponseKeeper() const = 0;

    virtual NDistributedThrottler::IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
        NDistributedThrottler::TDistributedThrottlerConfigPtr config,
        IInvokerPtr invoker,
        const std::string& groupId,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateCypressProxyBootstrap(
    TCypressProxyBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
