#include "client_impl.h"
#include "config.h"

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/ytlib/query_tracker_client/config.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>
#include <yt/yt/ytlib/query_tracker_client/query_tracker_service_proxy.h>

namespace NYT::NApi::NNative {

using namespace NRpcProxy;
using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NQueryClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TQueryId TClient::DoStartQuery(EQueryEngine engine, const TString& query, const TStartQueryOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.StartQuery();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    if (options.Settings) {
        rpcRequest->set_settings(ConvertToYsonString(options.Settings).ToString());
    }
    if (options.Annotations) {
        rpcRequest->set_annotations(ConvertToYsonString(options.Annotations).ToString());
    }
    if (options.AccessControlObject) {
        rpcRequest->set_access_control_object(*options.AccessControlObject);
    }
    rpcRequest->set_draft(options.Draft);
    for (const auto& file : options.Files) {
        auto* protoFile = rpcRequest->add_files();
        protoFile->set_name(file->Name);
        protoFile->set_content(file->Content);
        protoFile->set_type(static_cast<NProto::EContentType>(file->Type));
    }

    if (options.AccessControlObjects) {
        auto* protoAccessControlObjects = rpcRequest->mutable_access_control_objects();
        for (const auto& aco : *options.AccessControlObjects) {
            protoAccessControlObjects->add_items(aco);
        }
    }

    rpcRequest->set_engine(NProto::ConvertQueryEngineToProto(engine));
    rpcRequest->set_query(query);

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();
    auto rpcResponse = rsp->rpc_proxy_response();

    return FromProto<TQueryId>(rpcResponse.query_id());
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAbortQuery(TQueryId queryId, const TAbortQueryOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.AbortQuery();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(rpcRequest->mutable_query_id(), queryId);
    if (options.AbortMessage) {
        rpcRequest->set_abort_message(*options.AbortMessage);
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TQueryResult TClient::DoGetQueryResult(TQueryId queryId, i64 resultIndex, const TGetQueryResultOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.GetQueryResult();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(rpcRequest->mutable_query_id(), queryId);
    rpcRequest->set_result_index(resultIndex);

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();
    auto rpcResponse = rsp->rpc_proxy_response();

    return TQueryResult {
        .Id = FromProto<TQueryId>(rpcResponse.query_id()),
        .ResultIndex = rpcResponse.result_index(),
        .Error = FromProto<TError>(rpcResponse.error()),
        .Schema = rpcResponse.has_schema() ? FromProto<TTableSchemaPtr>(rpcResponse.schema()) : nullptr,
        .DataStatistics = FromProto<TDataStatistics>(rpcResponse.data_statistics()),
        .IsTruncated = rpcResponse.is_truncated(),
    };
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowsetPtr TClient::DoReadQueryResult(TQueryId queryId, i64 resultIndex, const TReadQueryResultOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.ReadQueryResult();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(rpcRequest->mutable_query_id(), queryId);
    rpcRequest->set_result_index(resultIndex);

    if (options.Columns) {
        auto* protoColumns = rpcRequest->mutable_columns();
        for (const auto& column : *options.Columns) {
            protoColumns->add_items(column);
        }
    }
    if (options.LowerRowIndex) {
        rpcRequest->set_lower_row_index(*options.LowerRowIndex);
    }
    if (options.UpperRowIndex) {
        rpcRequest->set_upper_row_index(*options.UpperRowIndex);
    }

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();
    auto rpcResponse = rsp->rpc_proxy_response();

    struct TQueryTrackerNativeClientTag { };
    return NRpcProxy::DeserializeRowset<TUnversionedRow>(
        rpcResponse.rowset_descriptor(),
        MergeRefsToRef<TQueryTrackerNativeClientTag>(rsp->Attachments()));
}

////////////////////////////////////////////////////////////////////////////////

TQuery TClient::DoGetQuery(TQueryId queryId, const TGetQueryOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.GetQuery();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(rpcRequest->mutable_query_id(), queryId);
    if (options.Attributes) {
        ToProto(rpcRequest->mutable_attributes(), options.Attributes);
    }
    if (options.Timestamp) {
        rpcRequest->set_timestamp(ToProto<i64>(options.Timestamp));
    }

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();
    auto rpcResponse = rsp->rpc_proxy_response();
    return FromProto<TQuery>(rpcResponse.query());
}

////////////////////////////////////////////////////////////////////////////////

TListQueriesResult TClient::DoListQueries(const TListQueriesOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.ListQueries();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    if (options.FromTime) {
        rpcRequest->set_from_time(NYT::ToProto<i64>(*options.FromTime));
    }
    if (options.ToTime) {
        rpcRequest->set_to_time(NYT::ToProto<i64>(*options.ToTime));
    }
    if (options.CursorTime) {
        rpcRequest->set_cursor_time(NYT::ToProto<i64>(*options.CursorTime));
    }

    rpcRequest->set_cursor_direction(static_cast<NProto::EOperationSortDirection>(options.CursorDirection));

    if (options.UserFilter) {
        rpcRequest->set_user_filter(*options.UserFilter);
    }
    if (options.StateFilter) {
        rpcRequest->set_state_filter(NProto::ConvertQueryStateToProto(*options.StateFilter));
    }
    if (options.EngineFilter) {
        rpcRequest->set_engine_filter(NProto::ConvertQueryEngineToProto(*options.EngineFilter));
    }
    if (options.SubstrFilter) {
        rpcRequest->set_substr_filter(*options.SubstrFilter);
    }

    rpcRequest->set_limit(options.Limit);

    if (options.Attributes) {
        ToProto(rpcRequest->mutable_attributes(), options.Attributes);
    }

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();
    auto rpcResponse = rsp->rpc_proxy_response();

    std::vector<TQuery> queries(rpcResponse.queries_size());
    for (auto i = 0; i < rpcResponse.queries_size(); i++) {
        FromProto(&queries[i], rpcResponse.queries()[i]);
    }
    return TListQueriesResult{
        .Queries = std::move(queries),
        .Incomplete = rpcResponse.incomplete(),
        .Timestamp = rpcResponse.timestamp(),
    };
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAlterQuery(TQueryId queryId, const TAlterQueryOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.AlterQuery();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    ToProto(rpcRequest->mutable_query_id(), queryId);
    if (options.Annotations) {
        rpcRequest->set_annotations(ConvertToYsonString(options.Annotations).ToString());
    }
    if (options.AccessControlObject) {
        rpcRequest->set_access_control_object(*options.AccessControlObject);
    }
    if (options.AccessControlObjects) {
        auto* protoAccessControlObjects = rpcRequest->mutable_access_control_objects();
        for (const auto& aco : *options.AccessControlObjects) {
            protoAccessControlObjects->add_items(aco);
        }
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetQueryTrackerInfoResult TClient::DoGetQueryTrackerInfo(const TGetQueryTrackerInfoOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.GetQueryTrackerInfo();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    auto* rpcRequest = req->mutable_rpc_proxy_request();
    rpcRequest->set_query_tracker_stage(options.QueryTrackerStage);
    if (options.Attributes) {
        ToProto(rpcRequest->mutable_attributes(), options.Attributes);
    }

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();
    auto rpcResponse = rsp->rpc_proxy_response();

    return TGetQueryTrackerInfoResult{
        .ClusterName = rpcResponse.cluster_name(),
        .SupportedFeatures = TYsonString(rpcResponse.supported_features()),
        .AccessControlObjects = FromProto<std::vector<TString>>(rpcResponse.access_control_objects()),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
