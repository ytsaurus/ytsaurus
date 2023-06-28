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
        context->SetRequestInfo("Async: %v, BuildRowsets: %v, RowCountLimit: %v", request->async(), request->build_rowsets(), request->row_count_limit());

        const auto& impersonationUser = context->GetAuthenticationIdentity().User;

        auto queryId = request->has_query_id()
            ? FromProto<TQueryId>(request->query_id())
            : TQueryId::Create();
        ToProto(response->mutable_query_id(), queryId);

        context->SetResponseInfo("QueryId: %v", queryId);

        auto responseFuture = YqlAgent_->StartQuery(queryId, impersonationUser, *request);

        if (request->async()) {
            // TODO(max42): there is no way to poll query result for now.
            context->Reply();
            return;
        }

        auto [builtResponse, refs] = WaitForUnique(responseFuture)
            .ValueOrThrow();

        response->MergeFrom(builtResponse);
        response->Attachments() = std::move(refs);

        context->Reply();
    }
};

IServicePtr CreateYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent)
{
    return New<TYqlService>(std::move(controlInvoker), std::move(yqlAgent));
}

////////////////////////////////////////////////////////////////////////////////

} // namepsace NYT::NYqlAgent
