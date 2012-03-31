#include "stdafx.h"
#include "scheduler_commands.h"

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NDriver {

using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TMapCommand::DoExecute(TMapRequestPtr request)
{
    auto transaction = Host->GetTransaction(request, true);

    TSchedulerServiceProxy proxy(Host->GetSchedulerChannel());

    TOperationId operationId;
    {

        auto startOpReq = proxy.StartOperation();
        *startOpReq->mutable_transaction_id() = transaction->GetId().ToProto();
        startOpReq->set_type(EOperationType::Map);
        startOpReq->set_transaction_id(transaction->GetId().ToProto());
        startOpReq->set_spec(SerializeToYson(~request->Spec));

        auto startOpRsp = startOpReq->Invoke()->Get();
        if (!startOpRsp->IsOK()) {
            Host->ReplyError(startOpRsp->GetError());
            return;
        }

        operationId = TOperationId::FromProto(startOpRsp->operation_id());
    }

    Host->ReplySuccess(BuildYsonFluently()
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
            Host->ReplyError(waitOpRsp->GetError());
            return;
        }

        auto error = TError::FromProto(waitOpRsp->result().error());
        if (error.IsOK()) {
            Host->ReplySuccess();
        } else {
            Host->ReplyError(error);
        }
    }

    // TODO(babenko): dump stderrs
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
