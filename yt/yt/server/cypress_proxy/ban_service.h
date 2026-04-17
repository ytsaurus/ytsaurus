#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_proxy/config.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class IBanService
    : public virtual TRefCounted
{
public:
    virtual bool IsBanned(const std::string& user) = 0;

    virtual NRpc::IServicePtr GetService() = 0;
    virtual void Reconfigure(const TBanServiceDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBanService);

IBanServicePtr CreateBanService(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // NYT::NCypressProxy
