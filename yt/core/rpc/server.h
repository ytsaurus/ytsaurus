#pragma once

#include "public.h"

#include <core/bus/public.h>

#include <core/ytree/public.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public virtual TRefCounted
{
    virtual void RegisterService(IServicePtr service) = 0;

    virtual void Configure(TServerConfigPtr config) = 0;

    virtual void Start() = 0;
    virtual void Stop() = 0;
};

IServerPtr CreateRpcServer(NBus::IBusServerPtr busServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
