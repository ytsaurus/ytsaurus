#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const TCypressProxyConfigPtr& GetConfig() const = 0;

    virtual const IInvokerPtr& GetControlInvoker() const = 0;

    virtual NApi::IClientPtr GetRootClient() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TCypressProxyConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
