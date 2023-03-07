#pragma once

#include "bootstrap.h"
#include "public.h"
#include "config.h"

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        const TBootstrap* bootstrap);
    ~TSecurityManager();

    void ValidateUser(const TString& user);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
