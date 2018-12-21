#pragma once

#include "public.h"

#include <yt/core/rpc/service_detail.h>

namespace NYP::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TServiceBase
    : public NRpc::TServiceBase
{
protected:
    TBootstrap* const Bootstrap_;

    TServiceBase(
        TBootstrap* bootstrap,
        const NYT::NRpc::TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger,
        NRpc::IAuthenticatorPtr authenticator = nullptr);

    virtual void BeforeInvoke(NRpc::IServiceContext* context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NMaster
