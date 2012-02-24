#pragma once

#include "common.h"
#include "service.h"

#include <ytlib/bus/server.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IServer> TPtr;

    virtual void RegisterService(IService* service) = 0;
    
    virtual void Start() = 0;
    virtual void Stop() = 0;

};

IServer::TPtr CreateRpcServer(NBus::IBusServer* busServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
