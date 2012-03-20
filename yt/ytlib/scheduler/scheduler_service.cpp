#include "stdafx.h"
#include "scheduler_service.h"
#include "private.h"

#include <ytlib/cell_scheduler/bootstrap.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

TSchedulerService::TSchedulerService(TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        ~bootstrap->GetControlInvoker(),
        TSchedulerServiceProxy::GetServiceName(),
        SchedulerLogger.GetCategory())
    , Bootstrap(bootstrap)
{
    YASSERT(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTask));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTask));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(WaitForTask));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, StartTask)
{
    // TODO(babenko): implement
    YUNIMPLEMENTED();
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, AbortTask)
{
    // TODO(babenko): implement
    YUNIMPLEMENTED();
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, WaitForTask)
{
    // TODO(babenko): implement
    YUNIMPLEMENTED();
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, Heartbeat)
{
    // TODO(babenko): implement
    YUNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
