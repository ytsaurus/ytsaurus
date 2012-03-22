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

    //// ... and wait until operation will complete
    //{
    //    auto request = proxy.WaitOperation();
    //    request->set_operation_id(operationId);
    //    request->SetTimeout(Null);
    //    auto future = request->Invoke();

    //    // this would block until scheduler answers us
    //    // which could be "immediatedly" in case of errors
    //    auto response = future->Get();
    //    if (response->IsOK()) {
    //        // OK means operation has finished successfully,
    //        // there are could be additional information that we must
    //        // pass to the user
    //        DriverImpl->ReplySuccess();
    //        // DriverImpl->ReplySuccess(NYTree::BuildYsonFluently()
    //        //     .BeginMap()
    //        //         .Item("status").Scalar("completed")
    //        //     .EndMap()
    //        //     .operator NYTree::TYson()
    //        //     );
    //    } else {
    //        // operation was unsuccessful -- may be there was
    //        // communication error or operation wasn't valid and therefore
    //        // has been rejected by the scheduler or it has been canceled
    //        // by whatever cause

    //        if (NRpc::IsRpcError(response->GetError())) {
    //            // communication error

    //            //TODO: engage some retry policy, give up for now
    //            DriverImpl->ReplyError(response->GetError());
    //        } else {
    //            // error from the scheduler

    //            DriverImpl->ReplyError(response->GetError());
    //        }
    //    }
    //}

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
