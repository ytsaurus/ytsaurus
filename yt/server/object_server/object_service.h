#pragma once

#include "public.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/rpc/service.h>

#include <server/cell_master/meta_state_service.h>

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

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
