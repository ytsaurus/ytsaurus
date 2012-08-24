#pragma once

#include "public.h"
#include "object_service_proxy.h"

#include <ytlib/rpc/service.h>

#include <ytlib/cell_master/meta_state_service.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    explicit TObjectService(NCellMaster::TBootstrap* bootstrap);

private:
    typedef TObjectService TThis;
    class TExecuteSession;

    DECLARE_RPC_SERVICE_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
