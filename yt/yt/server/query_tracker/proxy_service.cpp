#include "private.h"
#include "proxy_service.h"
#include "query_tracker_proxy.h"

#include <yt/yt/server/lib/state_checker/state_checker.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/ytlib/query_tracker_client/proto/query_tracker_service.pb.h>

#include <yt/yt/ytlib/query_tracker_client/query_tracker_service_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NConcurrency;
using namespace NQueryTrackerClient;
using namespace NRpc;
using namespace NRpcProxy;
using namespace NComponentStateChecker;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

static const TYsonString EmptyMap = TYsonString(TString("{}"));

////////////////////////////////////////////////////////////////////////////////

class TProxyService
    : public TServiceBase
{
public:
    TProxyService(IInvokerPtr proxyInvoker, TQueryTrackerProxyPtr queryTracker, IComponentStateCheckerPtr ComponentStateChecker)
        : TServiceBase(
            std::move(proxyInvoker),
            TQueryTrackerServiceProxy::GetDescriptor(),
            QueryTrackerLogger())
        , QueryTracker_(std::move(queryTracker))
        , ComponentStateChecker_(std::move(ComponentStateChecker))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryResult));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadQueryResult));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListQueries));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryTrackerInfo));
    }

private:
    const TQueryTrackerProxyPtr QueryTracker_;
    const IComponentStateCheckerPtr ComponentStateChecker_;

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, StartQuery)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqStartQuery::GetDescriptor()->field_count() == 10);
        YT_VERIFY(NRpcProxy::NProto::TRspStartQuery::GetDescriptor()->field_count() == 1);

        auto rpcRequest = request->rpc_proxy_request();
        auto* rpcResponse = response->mutable_rpc_proxy_response();

        auto queryId = TQueryId::Create();
        ToProto(rpcResponse->mutable_query_id(), queryId);

        TStartQueryOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        if (rpcRequest.has_settings()) {
            options.Settings = ConvertToNode(TYsonStringBuf(rpcRequest.settings()));
        }
        if (rpcRequest.has_annotations()) {
            options.Annotations = ConvertToNode(TYsonStringBuf(rpcRequest.annotations()))->AsMap();
        }
        if (rpcRequest.has_access_control_object()) {
            options.AccessControlObject = rpcRequest.access_control_object();
        }
        options.AccessControlObjects = rpcRequest.has_access_control_objects()
            ? std::make_optional(FromProto<std::vector<TString>>(rpcRequest.access_control_objects().items()))
            : std::nullopt;

        options.Draft = rpcRequest.draft();

        for (const auto& requestFile : rpcRequest.files()) {
            auto file = New<TQueryFile>();
            file->Name = requestFile.name();
            file->Content = requestFile.content();
            file->Type = FromProto<EContentType>(requestFile.type());
            options.Files.emplace_back(file);
        }

        for (const auto& protoSecret : rpcRequest.secrets()) {
            auto secret = New<TQuerySecret>();
            secret->Id = protoSecret.id();
            secret->Category = protoSecret.category();
            secret->Subcategory = protoSecret.subcategory();
            secret->YPath = protoSecret.ypath();
            options.Secrets.emplace_back(std::move(secret));
        }

        auto engine = ConvertQueryEngineFromProto(rpcRequest.engine());
        auto query = rpcRequest.query();
        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo();

        QueryTracker_->StartQuery(queryId, engine, query, options, user);

        context->SetResponseInfo("QueryId: %v", queryId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, AbortQuery)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqAbortQuery::GetDescriptor()->field_count() == 3);
        YT_VERIFY(NRpcProxy::NProto::TRspAbortQuery::GetDescriptor()->field_count() == 0);

        auto rpcRequest = request->rpc_proxy_request();

        auto queryId = FromProto<TQueryId>(rpcRequest.query_id());

        TAbortQueryOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        options.AbortMessage = rpcRequest.has_abort_message()
            ? std::make_optional(rpcRequest.abort_message())
            : std::nullopt;

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v", queryId);

        QueryTracker_->AbortQuery(queryId, options, user);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, GetQueryResult)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqGetQueryResult::GetDescriptor()->field_count() == 3);
        YT_VERIFY(NRpcProxy::NProto::TRspGetQueryResult::GetDescriptor()->field_count() == 7);

        auto rpcRequest = request->rpc_proxy_request();
        auto* rpcResponse = response->mutable_rpc_proxy_response();

        auto queryId = FromProto<TQueryId>(rpcRequest.query_id());

        auto resultIndex = rpcRequest.result_index();
        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, ResultIndex: %v",
            queryId,
            resultIndex);

        auto queryResult = QueryTracker_->GetQueryResult(queryId, resultIndex, user);

        ToProto(rpcResponse->mutable_query_id(), queryResult.Id);
        rpcResponse->set_result_index(queryResult.ResultIndex);
        if (queryResult.Schema) {
            ToProto(rpcResponse->mutable_schema(), queryResult.Schema);
        }
        ToProto(rpcResponse->mutable_error(), queryResult.Error);
        rpcResponse->set_is_truncated(queryResult.IsTruncated);
        if (queryResult.FullResult) {
            rpcResponse->set_full_result(queryResult.FullResult.AsStringBuf());
        }
        ToProto(rpcResponse->mutable_data_statistics(), queryResult.DataStatistics);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, ReadQueryResult)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqReadQueryResult::GetDescriptor()->field_count() == 6);
        YT_VERIFY(NRpcProxy::NProto::TRspReadQueryResult::GetDescriptor()->field_count() == 1);

        auto rpcRequest = request->rpc_proxy_request();
        auto* rpcResponse = response->mutable_rpc_proxy_response();

        auto queryId = FromProto<TQueryId>(rpcRequest.query_id());

        TReadQueryResultOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        options.LowerRowIndex = rpcRequest.has_lower_row_index()
            ? std::make_optional(rpcRequest.lower_row_index())
            : std::nullopt;
        options.UpperRowIndex = rpcRequest.has_upper_row_index()
            ? std::make_optional(rpcRequest.upper_row_index())
            : std::nullopt;
        options.Columns = rpcRequest.has_columns()
            ? std::make_optional(FromProto<std::vector<std::string>>(rpcRequest.columns().items()))
            : std::nullopt;

        auto resultIndex = rpcRequest.result_index();
        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, ResultIndex: %v",
            queryId,
            resultIndex);

        auto rowset = QueryTracker_->ReadQueryResult(queryId, resultIndex, options, user);

        response->Attachments() = SerializeRowset(
            *rowset->GetSchema(),
            rowset->GetRows(),
            rpcResponse->mutable_rowset_descriptor());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, GetQuery)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqGetQuery::GetDescriptor()->field_count() == 4);
        YT_VERIFY(NRpcProxy::NProto::TRspGetQuery::GetDescriptor()->field_count() == 1);

        auto rpcRequest = request->rpc_proxy_request();
        auto* rpcResponse = response->mutable_rpc_proxy_response();

        auto queryId = FromProto<TQueryId>(rpcRequest.query_id());

        TGetQueryOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        options.Attributes = rpcRequest.has_attributes()
            ? FromProto<TAttributeFilter>(rpcRequest.attributes())
            : TAttributeFilter();
        options.Timestamp = rpcRequest.has_timestamp()
            ? rpcRequest.timestamp()
            : NullTimestamp;

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v", queryId);

        auto query = QueryTracker_->GetQuery(queryId, options, user);
        ToProto(rpcResponse->mutable_query(), query);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, ListQueries)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqListQueries::GetDescriptor()->field_count() == 11);
        YT_VERIFY(NRpcProxy::NProto::TRspListQueries::GetDescriptor()->field_count() == 3);

        auto rpcRequest = request->rpc_proxy_request();
        auto* rpcResponse = response->mutable_rpc_proxy_response();

        TListQueriesOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        options.FromTime = rpcRequest.has_from_time()
            ? std::make_optional(TInstant::FromValue(rpcRequest.from_time()))
            : std::nullopt;

        options.ToTime = rpcRequest.has_to_time()
            ? std::make_optional(TInstant::FromValue(rpcRequest.to_time()))
            : std::nullopt;

        options.CursorTime = rpcRequest.has_cursor_time()
            ? std::make_optional(TInstant::FromValue(rpcRequest.cursor_time()))
            : std::nullopt;

        options.CursorDirection = FromProto<EOperationSortDirection>(rpcRequest.cursor_direction());

        options.UserFilter = rpcRequest.has_user_filter()
            ? std::make_optional(rpcRequest.user_filter())
            : std::nullopt;

        options.StateFilter = rpcRequest.has_state_filter()
            ? std::make_optional(ConvertQueryStateFromProto(rpcRequest.state_filter()))
            : std::nullopt;

        options.EngineFilter = rpcRequest.has_engine_filter()
            ? std::make_optional(ConvertQueryEngineFromProto(rpcRequest.engine_filter()))
            : std::nullopt;

        options.SubstrFilter = rpcRequest.has_substr_filter()
            ? std::make_optional(rpcRequest.substr_filter())
            : std::nullopt;

        options.Limit = rpcRequest.limit();

        options.Attributes = rpcRequest.has_attributes()
            ? FromProto<TAttributeFilter>(rpcRequest.attributes())
            : TAttributeFilter();

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo();

        auto result = QueryTracker_->ListQueries(options, user);

        for (const auto& query : result.Queries) {
            ToProto(rpcResponse->add_queries(), query);
        }
        rpcResponse->set_incomplete(result.Incomplete);
        rpcResponse->set_timestamp(result.Timestamp);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, AlterQuery)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqAlterQuery::GetDescriptor()->field_count() == 5);
        YT_VERIFY(NRpcProxy::NProto::TRspAlterQuery::GetDescriptor()->field_count() == 0);

        auto rpcRequest = request->rpc_proxy_request();
        auto queryId = FromProto<TQueryId>(rpcRequest.query_id());

        TAlterQueryOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        options.Annotations = rpcRequest.has_annotations()
            ? ConvertToNode(TYsonStringBuf(rpcRequest.annotations()))->AsMap()
            : nullptr;

        options.AccessControlObject = rpcRequest.has_access_control_object()
            ? std::make_optional(rpcRequest.access_control_object())
            : std::nullopt;

        options.AccessControlObjects = rpcRequest.has_access_control_objects()
            ? std::make_optional(FromProto<std::vector<TString>>(rpcRequest.access_control_objects().items()))
            : std::nullopt;

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v", queryId);

        QueryTracker_->AlterQuery(queryId, options, user);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, GetQueryTrackerInfo)
    {
        YT_VERIFY(NRpcProxy::NProto::TReqGetQueryTrackerInfo::GetDescriptor()->field_count() == 2);
        YT_VERIFY(NRpcProxy::NProto::TRspGetQueryTrackerInfo::GetDescriptor()->field_count() == 5);

        auto rpcRequest = request->rpc_proxy_request();
        auto* rpcResponse = response->mutable_rpc_proxy_response();

        TGetQueryTrackerInfoOptions options;
        if (rpcRequest.has_query_tracker_stage()) {
            options.QueryTrackerStage = rpcRequest.query_tracker_stage();
        }
        if (rpcRequest.has_attributes()) {
            options.Attributes = FromProto<TAttributeFilter>(rpcRequest.attributes());
        }
        context->SetRequestInfo();

        auto result = QueryTracker_->GetQueryTrackerInfo(options);

        rpcResponse->set_query_tracker_stage(result.QueryTrackerStage);
        rpcResponse->set_cluster_name(result.ClusterName);
        rpcResponse->set_supported_features(result.SupportedFeatures.ToString());
        for (const auto& accessControlObject : result.AccessControlObjects) {
            *rpcResponse->add_access_control_objects() = accessControlObject;
        }
        for (const auto& cluster : result.Clusters) {
            *rpcResponse->add_clusters() = cluster;
        }

        context->Reply();
    }

    bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return !ComponentStateChecker_->IsComponentBanned();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateProxyService(IInvokerPtr proxyInvoker, TQueryTrackerProxyPtr queryTracker, IComponentStateCheckerPtr ComponentStateChecker)
{
    return New<TProxyService>(std::move(proxyInvoker), std::move(queryTracker), std::move(ComponentStateChecker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
