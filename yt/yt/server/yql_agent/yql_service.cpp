#include "yql_service.h"

#include "yql_agent.h"

#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>

#include <yt/yt/ytlib/yql_client/proto/yql_service.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYqlClient;

////////////////////////////////////////////////////////////////////////////////

class TYqlService
    : public TServiceBase
{
public:
    TYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent)
        : TServiceBase(
            std::move(controlInvoker),
            TYqlServiceProxy::GetDescriptor(),
            YqlAgentLogger)
        , YqlAgent_(std::move(yqlAgent))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartQuery));
    }

private:
    const IYqlAgentPtr YqlAgent_;

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, StartQuery)
    {
        context->SetRequestInfo("Async: %v", request->async());

        auto queryId = TQueryId::Create();
        ToProto(response->mutable_query_id(), queryId);

        context->SetResponseInfo("QueryId: %v", queryId);

        auto responseFuture = YqlAgent_->StartQuery(queryId, request->yql_request());

        if (request->async()) {
            // TODO(max42): there is no way to poll query result for now.
            context->Reply();
            return;
        }

        auto responseOrError = WaitFor(responseFuture);
        if (responseOrError.IsOK()) {
            response->mutable_yql_response()->Swap(&responseOrError.Value());
        } else {
            ToProto(response->mutable_error(), static_cast<TError>(responseOrError));
        }

        context->Reply();
    }
};

IServicePtr CreateYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent)
{
    return New<TYqlService>(std::move(controlInvoker), std::move(yqlAgent));
}

////////////////////////////////////////////////////////////////////////////////

} // namepsace NYT::NYqlAgent
