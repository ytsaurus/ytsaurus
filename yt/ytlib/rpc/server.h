#pragma once

#include "common.h"
#include "service.h"

#include <ytlib/bus/server.h>
#include <ytlib/ytree/ytree_fwd.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IRpcServer
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IRpcServer> TPtr;

    virtual void RegisterService(IService* service) = 0;
    
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer) = 0;

};

IRpcServer::TPtr CreateRpcServer(NBus::IBusServer* busServer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
