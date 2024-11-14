#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const TCypressProxyConfigPtr& GetConfig() const = 0;

    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;

    virtual const TUserDirectoryPtr& GetUserDirectory() const = 0;
    virtual const IUserDirectorySynchronizerPtr& GetUserDirectorySynchronizer() const = 0;

    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;

    virtual const IInvokerPtr& GetControlInvoker() const = 0;

    virtual const NApi::NNative::IConnectionPtr& GetNativeConnection() const = 0;
    virtual const NApi::NNative::IClientPtr& GetNativeRootClient() const = 0;

    virtual NSequoiaClient::ISequoiaClientPtr GetSequoiaClient() const = 0;

    virtual const NApi::NNative::IConnectionPtr& GetGroundConnection() const = 0;
    virtual const NApi::NNative::IClientPtr& GetGroundRootClient() const = 0;
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

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TCypressProxyConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
