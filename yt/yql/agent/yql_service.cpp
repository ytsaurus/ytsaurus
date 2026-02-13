#include "yql_service.h"

#include "yql_agent.h"

#include <yt/yt/server/lib/component_state_checker/state_checker.h>

#include <yt/yt/ytlib/yql_client/public.h>
#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>

#include <yt/yt/ytlib/yql_client/proto/yql_service.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NYqlAgent {

using namespace NConcurrency;
using namespace NRpc;
using namespace NComponentStateChecker;
using namespace NYqlClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TYqlService
    : public TServiceBase
{
public:
    TYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent, IComponentStateCheckerPtr ComponentStateChecker)
        : TServiceBase(
            std::move(controlInvoker),
            TYqlServiceProxy::GetDescriptor(),
            YqlAgentLogger())
        , YqlAgent_(std::move(yqlAgent))
        , ComponentStateChecker_(std::move(ComponentStateChecker))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartQuery)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetDeclaredParametersInfo));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryProgress));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetYqlAgentInfo));
    }

private:
    const IYqlAgentPtr YqlAgent_;
    const IComponentStateCheckerPtr ComponentStateChecker_;

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, StartQuery)
    {
        // TODO(babenko): switch to std::string
        auto user = TString(context->GetAuthenticationIdentity().User);

        auto queryId = request->has_query_id()
            ? FromProto<TQueryId>(request->query_id())
            : TQueryId::Create();
        ToProto(response->mutable_query_id(), queryId);

        context->SetRequestInfo("QueryId: %v, Async: %v, BuildRowsets: %v, RowCountLimit: %v", queryId, request->async(), request->build_rowsets(), request->row_count_limit());
        context->SetResponseInfo("QueryId: %v", queryId);

        if (ComponentStateChecker_->IsComponentBanned()) {
            YT_LOG_INFO("Yql agent is banned, failing query (QueryId: %v, User: %v)", queryId, user);
            THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::YqlAgentBanned, "Yql agent is banned");
        }

        auto responseFuture = YqlAgent_->StartQuery(queryId, user, *request);

        context->SubscribeCanceled(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            YT_LOG_INFO(error, "Request is canceled, aborting query (QueryId: %v)", queryId);

            YqlAgent_->AbortQuery(queryId).Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (error.IsOK()) {
                    YT_LOG_INFO("Query abort finished (QueryId: %v)", queryId);
                } else {
                    YT_LOG_ERROR(error, "Failed to abort query (QueryId: %v)", queryId);
                }
            }));
        }));

        if (request->async()) {
            // TODO(max42): there is no way to poll query result for now.
            context->Reply();
            return;
        }

        auto [builtResponse, refs] = WaitFor(responseFuture.AsUnique())
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

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, GetDeclaredParametersInfo)
    {
        // TODO(babenko): switch to std::string
        auto user = TString(context->GetAuthenticationIdentity().User);

        context->SetRequestInfo();
        context->SetResponseInfo();

        static const auto EmptyMap = TYsonString(TString("{}"));
        auto responseFuture = YqlAgent_->GetDeclaredParametersInfo(user, request->query(), request->has_settings() ? TYsonString(request->settings()) : EmptyMap);

        auto result = WaitFor(responseFuture.AsUnique())
            .ValueOrThrow();

        response->MergeFrom(result);

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

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, GetYqlAgentInfo)
    {
        response->MergeFrom(YqlAgent_->GetYqlAgentInfo());

        context->SetRequestInfo();
        context->SetResponseInfo();

        context->Reply();
    }

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return !ComponentStateChecker_->IsComponentBanned();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent, IComponentStateCheckerPtr ComponentStateChecker)
{
    return New<TYqlService>(std::move(controlInvoker), std::move(yqlAgent), std::move(ComponentStateChecker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
