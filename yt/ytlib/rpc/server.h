#pragma once

#include "common.h"
#include "service.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IServer> TPtr;

    virtual void RegisterService(IService* service) = 0;
    
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual Stroka GetDebugInfo() = 0;
};

IServer::TPtr CreateRpcServer(int port);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
