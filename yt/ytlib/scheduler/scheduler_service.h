#pragma once

#include "public.h"
#include "scheduler_service_proxy.h"

#include <ytlib/rpc/service.h>
#include <ytlib/rpc/channel_cache.h>
#include <ytlib/cell_scheduler/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerService
    : public NRpc::TServiceBase
{
public:
    TSchedulerService(NCellScheduler::TBootstrap* bootstrap);

private:
    typedef TSchedulerService TThis;

    NCellScheduler::TBootstrap* Bootstrap;

    DECLARE_RPC_SERVICE_METHOD(NProto, StartOperation);
    DECLARE_RPC_SERVICE_METHOD(NProto, AbortOperation);
    DECLARE_RPC_SERVICE_METHOD(NProto, WaitForOperation);

    DECLARE_RPC_SERVICE_METHOD(NProto, Heartbeat);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
