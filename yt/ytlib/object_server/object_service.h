#pragma once

#include "public.h"
#include "object_service_proxy.h"

#include <ytlib/rpc/service.h>

#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectService
    : public NRpc::TServiceBase
{
public:
    explicit TObjectService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TObjectService TThis;
    class TExecuteSession;

    NCellMaster::TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
