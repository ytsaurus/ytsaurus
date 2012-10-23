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
    explicit TObjectService(
        TObjectManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

private:
    typedef TObjectService TThis;
    class TExecuteSession;

    TObjectManagerConfigPtr Config;

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, GcCollect);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
