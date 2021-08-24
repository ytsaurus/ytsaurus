#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        IBootstrap* bootstrap,
        NLogging::TLogger logger);
    ~TSecurityManager();

    void ValidateUser(const TString& user);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
