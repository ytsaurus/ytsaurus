#include "stdafx.h"
#include "scheduler_service.h"
#include "private.h"
#include "scheduler.h"

#include <ytlib/cell_scheduler/bootstrap.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NScheduler {

using namespace NRpc;
using namespace NCellScheduler;
using namespace NYTree;

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

    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartOperation));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortOperation));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(WaitForOperation));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, StartOperation)
{
    auto type = EOperationType(request->type());
    auto transactionId = TTransactionId::FromProto(request->transaction_id());
    auto spec = DeserializeFromYson(request->spec());

    context->SetRequestInfo("Type: %s, TransactionId: %s",
        ~type.ToString(),
        ~transactionId.ToString());


}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, AbortOperation)
{
    // TODO(babenko): implement
    YUNIMPLEMENTED();
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, WaitForOperation)
{
    // TODO(babenko): implement
    YUNIMPLEMENTED();
}

DEFINE_RPC_SERVICE_METHOD(TSchedulerService, Heartbeat)
{
    auto address = request->address();

    context->SetRequestInfo("Address: %s, JobCount: %d, TotalSlotCount: %d, FreeSlotCount: %d",
        ~address,
        request->jobs_size(),
        request->total_slot_count(),
        request->free_slot_count());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
