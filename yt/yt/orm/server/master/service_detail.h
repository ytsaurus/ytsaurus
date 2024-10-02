#pragma once

#include "public.h"

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TServiceBase
    : public NRpc::TServiceBase
{
protected:
    IBootstrap* const Bootstrap_;

    TServiceBase(
        IBootstrap* bootstrap,
        const NRpc::TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger,
        NRpc::IAuthenticatorPtr authenticator = nullptr,
        IInvokerPtr defaultInvoker = nullptr);

    void BeforeInvoke(NRpc::IServiceContext* context) final;

private:
    using TBase = NRpc::TServiceBase;

    void SetupTracing(const NYT::NRpc::IServiceContextPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
