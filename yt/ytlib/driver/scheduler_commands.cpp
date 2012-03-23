#include "stdafx.h"
#include "scheduler_commands.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TMapCommand::DoExecute(TMapRequest* request)
{
    auto transaction = DriverImpl->GetTransaction(request, true);

    TSchedulerServiceProxy proxy(DriverImpl->GetSchedulerChannel());

    TOperationId operationId;
    {

        auto startOpReq = proxy.StartOperation();
        *startOpReq->mutable_transaction_id() = transaction->GetId().ToProto();
        startOpReq->set_type(EOperationType::Map);
        startOpReq->set_transaction_id(transaction->GetId().ToProto());
        startOpReq->set_spec(SerializeToYson(~request->Spec));

        auto startOpRsp = startOpReq->Invoke()->Get();
        if (!startOpRsp->IsOK()) {
            DriverImpl->ReplyError(startOpRsp->GetError());
            return;
        }

        operationId = TOperationId::FromProto(startOpRsp->operation_id());
    }

    DriverImpl->ReplySuccess(BuildYsonFluently()
        .BeginMap()
            .Item("operation_id").Scalar(operationId.ToString())
        .EndMap());

    {
        auto waitOpReq = proxy.WaitForOperation();
        waitOpReq->set_operation_id(operationId.ToProto());

        // Operation can run for a while, override the default timeout.
        waitOpReq->SetTimeout(Null);
        auto waitOpRsp = waitOpReq->Invoke()->Get();

        if (!waitOpRsp->IsOK()) {
            DriverImpl->ReplyError(waitOpRsp->GetError());
            return;
        }
    }

    // TODO(babenko): dump stderrs
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
