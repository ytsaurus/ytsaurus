#include "private.h"
#include "proxy_service.h"
#include "query_tracker_proxy.h"

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/ytlib/query_tracker_client/proto/query_tracker_service.pb.h>

#include <yt/yt/ytlib/query_tracker_client/query_tracker_service_proxy.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NConcurrency;
using namespace NQueryTrackerClient;
using namespace NRpc;
using namespace NYTree;

static const TYsonString EmptyMap = TYsonString(TString("{}"));

////////////////////////////////////////////////////////////////////////////////

class TProxyService
    : public TServiceBase
{
public:
    TProxyService(IInvokerPtr proxyInvoker, TQueryTrackerProxyPtr queryTracker)
        : TServiceBase(
            std::move(proxyInvoker),
            TQueryTrackerServiceProxy::GetDescriptor(),
            QueryTrackerLogger)
        , QueryTracker_(std::move(queryTracker))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQueryResult));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReadQueryResult));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetQuery));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ListQueries));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AlterQuery));
    }

private:
    const TQueryTrackerProxyPtr QueryTracker_;

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, StartQuery)
    {
        auto queryId = TQueryId::Create();
        ToProto(response->mutable_query_id(), queryId);

        TStartQueryOptions options;
        if (request->has_settings()) {
            options.Settings = ConvertToNode(TYsonStringBuf(request->settings()));
        }
        if (request->has_annotations()) {
            options.Annotations = ConvertToNode(TYsonStringBuf(request->annotations()))->AsMap();
        }
        if (request->has_access_control_object()) {
            options.AccessControlObject = request->access_control_object();
        }
        options.Draft = request->draft();
        options.Files = ConvertTo<std::vector<TQueryFilePtr>>(TYsonStringBuf(request->files()));

        auto engine = static_cast<EQueryEngine>(request->engine());
        auto query = request->query();
        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, User: %v", queryId, user);
        context->SetResponseInfo("QueryId: %v", queryId);

        QueryTracker_->StartQuery(queryId, engine, query, options, user);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, AbortQuery)
    {
        TQueryId queryId;
        FromProto(&queryId, request->query_id());

        TAbortQueryOptions options;
        options.AbortMessage = request->has_abort_message()
            ? std::make_optional(request->abort_message())
            : std::nullopt;

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, User: %v", queryId, user);
        context->SetResponseInfo("QueryId: %v", queryId);

        QueryTracker_->AbortQuery(queryId, options, user);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, GetQueryResult)
    {
        TQueryId queryId;
        FromProto(&queryId, request->query_id());

        auto resultIndex = request->result_index();
        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, ResultIndex: %v, User: %v", queryId, resultIndex, user);
        context->SetResponseInfo("QueryId: %v", queryId);

        auto queryResult = QueryTracker_->GetQueryResult(queryId, resultIndex, user);

        ToProto(response->mutable_query_id(), queryResult.Id);
        response->set_result_index(queryResult.ResultIndex);
        if (queryResult.Schema) {
            ToProto(response->mutable_schema(), queryResult.Schema);
        }
        ToProto(response->mutable_error(), queryResult.Error);
        response->set_is_truncated(queryResult.IsTruncated);
        ToProto(response->mutable_data_statistics(), queryResult.DataStatistics);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, ReadQueryResult)
    {
        TQueryId queryId;
        FromProto(&queryId, request->query_id());

        TReadQueryResultOptions options;
        options.LowerRowIndex = request->has_lower_row_index()
            ? std::make_optional(request->lower_row_index())
            : std::nullopt;
        options.UpperRowIndex = request->has_upper_row_index()
            ? std::make_optional(request->upper_row_index())
            : std::nullopt;
        options.Columns = request->has_columns()
            ? std::make_optional(FromProto<std::vector<TString>>(request->columns().items()))
            : std::nullopt;

        auto resultIndex = request->result_index();
        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, ResultIndex: %v, User: %v", queryId, resultIndex, user);
        context->SetResponseInfo("QueryId: %v", queryId);

        auto rowset = QueryTracker_->ReadQueryResult(queryId, resultIndex, options, user);

        response->Attachments() = NRpcProxy::SerializeRowset(
            *rowset->GetSchema(),
            rowset->GetRows(),
            response->mutable_rowset_descriptor());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, GetQuery)
    {
        TQueryId queryId;
        FromProto(&queryId, request->query_id());

        TGetQueryOptions options;
        options.Attributes = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();
        options.Timestamp = request->has_timestamp()
            ? request->timestamp()
            : NTransactionClient::NullTimestamp;

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, User: %v", queryId, user);
        context->SetResponseInfo("QueryId: %v", queryId);

        auto query = QueryTracker_->GetQuery(queryId, options, user);
        ToProto(response->mutable_query(), query);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, ListQueries)
    {
        TListQueriesOptions options;
        options.FromTime = request->has_from_time()
            ? std::make_optional(TInstant::FromValue(request->from_time()))
            : std::nullopt;

        options.ToTime = request->has_to_time()
            ? std::make_optional(TInstant::FromValue(request->to_time()))
            : std::nullopt;

        options.CursorTime = request->has_cursor_time()
            ? std::make_optional(TInstant::FromValue(request->cursor_time()))
            : std::nullopt;

        options.CursorDirection = FromProto<EOperationSortDirection>(request->cursor_direction());

        options.UserFilter = request->has_user_filter()
            ? std::make_optional(request->user_filter())
            : std::nullopt;

        options.StateFilter = request->has_state_filter()
            ? std::make_optional(FromProto<EQueryState>(request->state_filter()))
            : std::nullopt;

        options.EngineFilter = request->has_engine_filter()
            ? std::make_optional(FromProto<EQueryEngine>(request->engine_filter()))
            : std::nullopt;

        options.SubstrFilter = request->has_substr_filter()
            ? std::make_optional(request->substr_filter())
            : std::nullopt;

        options.Limit = request->limit();

        options.Attributes = request->has_attributes()
            ? FromProto<TAttributeFilter>(request->attributes())
            : TAttributeFilter();

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("User: %v", context->GetAuthenticationIdentity().User);

        auto result = QueryTracker_->ListQueries(options, user);

        for (const auto& query : result.Queries) {
            ToProto(response->add_queries(), query);
        }
        response->set_incomplete(result.Incomplete);
        response->set_timestamp(result.Timestamp);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NQueryTrackerClient::NProto, AlterQuery)
    {
        TQueryId queryId;
        FromProto(&queryId, request->query_id());

        TAlterQueryOptions options;
        options.Annotations = request->has_annotations()
            ? ConvertToNode(TYsonStringBuf(request->annotations()))->AsMap()
            : nullptr;

        options.AccessControlObject = request->has_access_control_object()
            ? std::make_optional(request->access_control_object())
            : std::nullopt;

        auto user = context->GetAuthenticationIdentity().User;

        context->SetRequestInfo("QueryId: %v, User: %v", queryId, user);
        context->SetResponseInfo("QueryId: %v", queryId);

        QueryTracker_->AlterQuery(queryId, options, user);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateProxyService(IInvokerPtr proxyInvoker, TQueryTrackerProxyPtr queryTracker)
{
    return New<TProxyService>(std::move(proxyInvoker), std::move(queryTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
