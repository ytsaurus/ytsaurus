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

        context->AnnotateRequest()
            .With("QueryId", queryId)
            .With("Async", request->async())
            .With("BuildRowsets", request->build_rowsets())
            .With("RowCountLimit", request->row_count_limit());
        context->AnnotateResponse()
            .With("QueryId", queryId);

        if (ComponentStateChecker_->IsComponentBanned()) {
            YT_TLOG_INFO("YQL agent is banned; failing query")
                .With("QueryId", queryId)
                .With("User", user);
            THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::YqlAgentBanned, "Yql agent is banned");
        }

        if (!YqlAgent_->IsReady()) {
            YT_TLOG_INFO("YQL agent is not ready; failing query")
                .With("QueryId", queryId)
                .With("User", user);
            THROW_ERROR_EXCEPTION(NYqlClient::EErrorCode::YqlAgentNotReady, "Yql agent is not ready");
        }

        auto responseFuture = YqlAgent_->StartQuery(queryId, user, *request);

        context->SubscribeCanceled(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            YT_TLOG_INFO("Request is canceled, aborting query")
                .With("QueryId", queryId)
                .With(error);

            YqlAgent_->AbortQuery(queryId).Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (error.IsOK()) {
                    YT_TLOG_INFO("Query abort finished")
                        .With("QueryId", queryId);
                } else {
                    YT_TLOG_ERROR("Failed to abort query")
                        .With("QueryId", queryId)
                        .With(error);
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

        context->AnnotateRequest()
            .With("QueryId", queryId);
        context->AnnotateResponse()
            .With("QueryId", queryId);

        WaitFor(YqlAgent_->AbortQuery(queryId))
            .ThrowOnError();

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, GetDeclaredParametersInfo)
    {
        // TODO(babenko): switch to std::string
        auto user = TString(context->GetAuthenticationIdentity().User);

        context->AnnotateRequest();
        context->AnnotateResponse();

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

        context->AnnotateRequest()
            .With("QueryId", queryId);
        context->AnnotateResponse()
            .With("QueryId", queryId);

        response->MergeFrom(YqlAgent_->GetQueryProgress(queryId));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYqlClient::NProto, GetYqlAgentInfo)
    {
        response->MergeFrom(YqlAgent_->GetYqlAgentInfo());

        context->AnnotateRequest();
        context->AnnotateResponse();

        context->Reply();
    }

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return !ComponentStateChecker_->IsComponentBanned() && YqlAgent_->IsReady();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateYqlService(IInvokerPtr controlInvoker, IYqlAgentPtr yqlAgent, IComponentStateCheckerPtr ComponentStateChecker)
{
    return New<TYqlService>(std::move(controlInvoker), std::move(yqlAgent), std::move(ComponentStateChecker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
