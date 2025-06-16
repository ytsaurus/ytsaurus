#pragma once

#include "bootstrap.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TCypressProxyServiceContext
    : public NRpc::TTypedServiceContext<TRequestMessage, TResponseMessage>
{
public:
    using NRpc::TTypedServiceContext<TRequestMessage, TResponseMessage>::TTypedServiceContext;

    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, TargetMasterCellTag);
    DEFINE_BYVAL_RW_PROPERTY(NApi::EMasterChannelKind, TargetMasterChannelKind);
};

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyServiceBase
    : public NRpc::TServiceBase
{
public:
    TCypressProxyServiceBase(
        IBootstrap* bootstrap,
        IInvokerPtr defaultInvoker,
        const NRpc::TServiceDescriptor& descriptor,
        NLogging::TLogger logger,
        NRpc::TServiceOptions options = {});

    template <class TRequestMessage, class TResponseMessage>
    using TTypedServiceContextImpl = TCypressProxyServiceContext<TRequestMessage, TResponseMessage>;

    template <class TRequestMessage, class TResponseMessage>
    void InitContext(TCypressProxyServiceContext<TRequestMessage, TResponseMessage>* context);

    template <class TRequestMessage, class TResponseMessage>
    using TCypressProxyServiceContextPtr = TIntrusivePtr<TCypressProxyServiceContext<TRequestMessage, TResponseMessage>>;

    template <class TRequestMessage, class TResponseMessage>
    NRpc::IChannelPtr GetTargetMasterPeerChannelOrThrow(TCypressProxyServiceContextPtr<TRequestMessage, TResponseMessage> context) const;

protected:
    IBootstrap* const Bootstrap_;
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyServiceBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy

#define CYPRESS_PROXY_SERVICE_BASE_INL_H_
#include "cypress_proxy_service_base-inl.h"
#undef CYPRESS_PROXY_SERVICE_BASE_INL_H_
