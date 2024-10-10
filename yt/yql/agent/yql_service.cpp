#include "yql_service.h"

#include "yql_agent.h"

#include <yt/yt/server/lib/state_checker/state_checker.h>

#include <yt/yt/ytlib/yql_client/public.h>
#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>

#include <yt/yt/ytlib/yql_client/proto/yql_service.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NRpc;
using namespace NStateChecker;
using namespace NYqlClient;

////////////////////////////////////////////////////////////////////////////////

class TYqlService
    : public TServiceBase
{
public:
    TYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent, TStateCheckerPtr stateChecker)
        : TServiceBase(
            std::move(controlInvoker),
            TYqlServiceProxy::GetDescriptor(),
            YqlAgentLogger())
        , YqlAgent_(std::move(yqlAgent))
        , StateChecker_(std::move(stateChecker))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartQuery)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryProgress));
    }

private:
    const IYqlAgentPtr YqlAgent_;
    const TStateCheckerPtr StateChecker_;

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, StartQuery)
    {
        // TODO(babenko): switch to std::string
        auto impersonationUser = TString(context->GetAuthenticationIdentity().User);

        auto queryId = request->has_query_id()
            ? FromProto<TQueryId>(request->query_id())
            : TQueryId::Create();
        ToProto(response->mutable_query_id(), queryId);

        context->SetRequestInfo("QueryId: %v, Async: %v, BuildRowsets: %v, RowCountLimit: %v", queryId, request->async(), request->build_rowsets(), request->row_count_limit());
        context->SetResponseInfo("QueryId: %v", queryId);

        if (StateChecker_->IsComponentBanned()) {
            YT_LOG_INFO("Yql agent is banned, failing query (QueryId: %v, User: %v)", queryId, impersonationUser);
            THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::YqlAgentBanned, "Yql agent is banned");
        }

        auto responseFuture = YqlAgent_->StartQuery(queryId, impersonationUser, *request);

        context->SubscribeCanceled(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            YT_LOG_INFO(error, "Query is cancelled (QueryId: %v)", queryId);

            try {
                WaitFor(YqlAgent_->AbortQuery(queryId))
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                YT_LOG_ERROR(ex, "Failed to abort query (QueryId: %v)", queryId);
            }
        }));

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

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, AbortQuery)
    {
        auto queryId = FromProto<TQueryId>(request->query_id());

        context->SetRequestInfo("QueryId: %v", queryId);
        context->SetResponseInfo("QueryId: %v", queryId);

        WaitFor(YqlAgent_->AbortQuery(queryId))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, GetQueryProgress)
    {
        auto queryId = request->has_query_id()
            ? FromProto<TQueryId>(request->query_id())
            : TQueryId::Create();

        context->SetRequestInfo("QueryId: %v", queryId);
        context->SetResponseInfo("QueryId: %v", queryId);

        response->MergeFrom(YqlAgent_->GetQueryProgress(queryId));
        context->Reply();
    }

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return !StateChecker_->IsComponentBanned();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent, TStateCheckerPtr stateChecker)
{
    return New<TYqlService>(std::move(controlInvoker), std::move(yqlAgent), std::move(stateChecker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
