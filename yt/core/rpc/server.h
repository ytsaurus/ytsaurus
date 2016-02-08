#pragma once

#include "public.h"

#include <yt/core/bus/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public virtual TRefCounted
{
    virtual void RegisterService(IServicePtr service) = 0;
    virtual void UnregisterService(IServicePtr service) = 0;

    virtual IServicePtr FindService(const TServiceId& serviceId) = 0;

    virtual void Configure(TServerConfigPtr config) = 0;

    virtual void Start() = 0;
    virtual TFuture<void> Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IServer)

IServerPtr CreateBusServer(NBus::IBusServerPtr busServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
