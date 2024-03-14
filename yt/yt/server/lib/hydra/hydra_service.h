#pragma once

#include "public.h"

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IUpstreamSynchronizer
    : public TRefCounted
{
    virtual TFuture<void> SyncWithUpstream() = 0;
};

DEFINE_REFCOUNTED_TYPE(IUpstreamSynchronizer)

////////////////////////////////////////////////////////////////////////////////

IUpstreamSynchronizerPtr CreateHydraManagerUpstreamSynchronizer(TWeakPtr<IHydraManager> hydraManager);

////////////////////////////////////////////////////////////////////////////////

class THydraServiceBase
    : public NRpc::TServiceBase
{
protected:
    THydraServiceBase(
        IHydraManagerPtr hydraManager,
        IInvokerPtr defaultInvoker,
        const NRpc::TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger,
        NRpc::TRealmId realmId,
        IUpstreamSynchronizerPtr upstreamSynchronizer,
        NRpc::IAuthenticatorPtr authenticator);

    void ValidatePeer(EPeerKind kind);
    void SyncWithUpstream();

private:
    const TWeakPtr<IHydraManager> HydraManager_;
    const IUpstreamSynchronizerPtr UpstreamSynchronizer_;

    bool IsUp(const TCtxDiscoverPtr& context) override;
    void EnrichDiscoverResponse(TRspDiscover* response) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
