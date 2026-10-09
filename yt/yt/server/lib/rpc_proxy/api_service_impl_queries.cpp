#include "api_service_impl.h"

#include <yt/yt/ytlib/query_tracker_client/query_tracker_service_proxy.h>

#include <yt/yt/client/api/query_tracker_client.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NQueryTrackerClient;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterQueryMethods()
{
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(StartQuery));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AbortQuery));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetQueryResult));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ReadQueryResult));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetQuery));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(ListQueries));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AlterQuery));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetQueryTrackerInfo));
    RegisterApiMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(GetQueryDeclaredParametersInfo));
}

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
TQueryTrackerServiceProxy TApiService::GetQueryTrackerProxy(
    const IServiceContextPtr& context,
    const TRequest* request)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);
    return TQueryTrackerServiceProxy(
        client->GetNativeConnection()->GetQueryTrackerChannelOrThrow(request->query_tracker_stage()));
}

template <class TProxyRequest, class TQTRequestPtr>
void FillQueryTrackerRequest(
    const IServiceContextPtr& context,
    const TProxyRequest* proxyRequest,
    const TQTRequestPtr qtRequest)
{
    qtRequest->SetTimeout(context->GetTimeout());
    qtRequest->SetUser(context->GetAuthenticationIdentity().User);
    qtRequest->mutable_rpc_proxy_request()->MergeFrom(*proxyRequest);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, StartQuery)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.StartQuery();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("Engine", ConvertQueryEngineFromProto(request->engine()));

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());

            context->AnnotateResponse()
                .With("QueryId", response->query_id());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AbortQuery)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.AbortQuery();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("QueryId", FromProto<NQueryTrackerClient::TQueryId>(request->query_id()));

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& /*context*/, const auto& /*result*/) {
            // do nothing.
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetQueryResult)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.GetQueryResult();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("QueryId", FromProto<NQueryTrackerClient::TQueryId>(request->query_id()))
        .With("ResultIndex", request->result_index());

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());

            context->AnnotateResponse()
                .With("QueryId", response->query_id());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ReadQueryResult)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.ReadQueryResult();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("QueryId", FromProto<NQueryTrackerClient::TQueryId>(request->query_id()))
        .With("ResultIndex", request->result_index());

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());
            response->Attachments() = result->Attachments();
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetQuery)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.GetQuery();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("QueryId", FromProto<NQueryTrackerClient::TQueryId>(request->query_id()))
        .With("StartTimestamp", request->timestamp());

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());

            context->AnnotateResponse()
                .With("QueryId", response->query().id());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ListQueries)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.ListQueries();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("Limit", request->limit());

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());

            context->AnnotateResponse()
                .With("QueryCount", response->queries_size())
                .With("Incomplete", response->incomplete())
                .With("Timestamp", response->timestamp());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AlterQuery)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.AlterQuery();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage())
        .With("QueryId", FromProto<NQueryTrackerClient::TQueryId>(request->query_id()));

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& /*context*/, const auto& /*result*/) {
            // do nothing.
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetQueryTrackerInfo)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.GetQueryTrackerInfo();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage());

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());

            context->AnnotateResponse()
                .With("ClusterName", response->cluster_name());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, GetQueryDeclaredParametersInfo)
{
    auto proxy = GetQueryTrackerProxy(context, request);

    auto req = proxy.GetQueryDeclaredParametersInfo();
    FillQueryTrackerRequest(context, request, req);

    context->AnnotateRequest()
        .With("Stage", request->query_tracker_stage());

    ExecuteCall(
        context,
        [=] {
            return req->Invoke();
        },
        [] (const auto& context, const auto& result) {
            auto* response = &context->Response();
            response->MergeFrom(result->rpc_proxy_response());

            context->AnnotateResponse();
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
