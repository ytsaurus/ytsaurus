#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any.
class TSecurityManager
{
public:
    TSecurityManager(
        TSecurityManagerDynamicConfigPtr config,
        IBootstrap* bootstrap,
        NLogging::TLogger logger);
    ~TSecurityManager();

    void ValidateUser(const TString& user);

    void Reconfigure(const TSecurityManagerDynamicConfigPtr& config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
