#pragma once

#include "public.h"

#include <core/bus/public.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IRpcServer
    : public virtual TRefCounted
{
    virtual void RegisterService(IServicePtr service) = 0;
    virtual void UnregisterService(IServicePtr service) = 0;

    virtual void Configure(TServerConfigPtr config) = 0;

    virtual void Start() = 0;
    virtual void Stop() = 0;
};

IRpcServerPtr CreateRpcServer(NBus::IBusServerPtr busServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
