#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/ytlib/sequoia_client/public.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

class ICypressProxyTracker
    : public virtual TRefCounted
{
public:
    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NProto::TReqHeartbeat,
        NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;
    /*!
     *  Thread affinity: any.
     */
    virtual void ProcessCypressProxyHeartbeat(const TCtxHeartbeatPtr& context) = 0;
    virtual bool TryProcessCypressProxyHeartbeatWithoutMutation(const TCtxHeartbeatPtr& context) = 0;

    virtual void Initialize() = 0;

    virtual TCypressProxyObject* FindCypressProxyByAddress(const std::string& address) = 0;

    virtual NRpc::IChannelPtr GetCypressProxyChannelOrThrow(const std::string& address) = 0;

    DECLARE_INTERFACE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(CypressProxy, CypressProxies, TCypressProxyObject);

private:
    friend class TCypressProxyTypeHandler;

    virtual void ZombifyCypressProxy(TCypressProxyObject* proxyObject) noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressProxyTracker)

////////////////////////////////////////////////////////////////////////////////

ICypressProxyTrackerPtr CreateCypressProxyTracker(
    NCellMaster::TBootstrap* bootstrap,
    NRpc::IChannelFactoryPtr channelFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
