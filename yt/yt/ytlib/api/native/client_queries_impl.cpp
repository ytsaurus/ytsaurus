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

namespace NDetail {

// Path to access control object namespace for QT.
constexpr TStringBuf QueriesAcoNamespacePath = "//sys/access_control_object_namespaces/queries";

} // namespace NDetail

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

TQueryId TClient::DoStartQuery(EQueryEngine engine, const TString& query, const TStartQueryOptions& options)
{
    auto queryTrackerConfig = Connection_->GetConfig()->QueryTracker;
    if (ssize(options.Files) > queryTrackerConfig->MaxQueryFileCount) {
        THROW_ERROR_EXCEPTION("Too many files: limit is %v, actual count is %v",
            queryTrackerConfig->MaxQueryFileCount,
            options.Files.size());
    }
    for (const auto& file : options.Files) {
        if (ssize(file->Name) > queryTrackerConfig->MaxQueryFileNameSizeBytes) {
            THROW_ERROR_EXCEPTION("Too large file %v name: limit is %v, actual size is %v",
                file->Name,
                queryTrackerConfig->MaxQueryFileNameSizeBytes,
                file->Name.size());
        }
        if (ssize(file->Content) > queryTrackerConfig->MaxQueryFileContentSizeBytes) {
            THROW_ERROR_EXCEPTION("Too large file %v content: limit is %v, actual size is %v",
                file->Name,
                queryTrackerConfig->MaxQueryFileContentSizeBytes,
                file->Content.size());
        }
    }

    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.StartQuery();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    if (options.Settings) {
        req->set_settings(ConvertToYsonString(options.Settings).ToString());
    }
    if (options.Annotations) {
        req->set_annotations(ConvertToYsonString(options.Annotations).ToString());
    }
    if (options.AccessControlObject) {
        req->set_access_control_object(*options.AccessControlObject);
    }
    req->set_draft(options.Draft);
    req->set_files(ConvertToYsonString(options.Files).ToString());

    req->set_engine(NProto::ConvertQueryEngineToProto(engine));
    req->set_query(query);

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();

    return FromProto<TQueryId>(rsp->query_id());
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAbortQuery(TQueryId queryId, const TAbortQueryOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.AbortQuery();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    ToProto(req->mutable_query_id(), queryId);
    if (options.AbortMessage) {
        req->set_abort_message(*options.AbortMessage);
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

    ToProto(req->mutable_query_id(), queryId);
    req->set_result_index(resultIndex);

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();

    return TQueryResult {
        .Id = FromProto<TQueryId>(rsp->query_id()),
        .ResultIndex = rsp->result_index(),
        .Error = FromProto<TError>(rsp->error()),
        .Schema = rsp->has_schema() ? FromProto<TTableSchemaPtr>(rsp->schema()) : nullptr,
        .DataStatistics = FromProto<TDataStatistics>(rsp->data_statistics()),
        .IsTruncated = rsp->is_truncated(),
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

    ToProto(req->mutable_query_id(), queryId);
    req->set_result_index(resultIndex);

    if (options.Columns) {
        auto* protoColumns = req->mutable_columns();
        for (const auto& column : *options.Columns) {
            protoColumns->add_items(column);
        }
    }
    if (options.LowerRowIndex) {
        req->set_lower_row_index(*options.LowerRowIndex);
    }
    if (options.UpperRowIndex) {
        req->set_upper_row_index(*options.UpperRowIndex);
    }

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();
    struct TQueryTrackerNativeClientTag { };
    return NRpcProxy::DeserializeRowset<TUnversionedRow>(
        rsp->rowset_descriptor(),
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

    ToProto(req->mutable_query_id(), queryId);
    if (options.Attributes) {
        ToProto(req->mutable_attributes(), options.Attributes);
    }
    if (options.Timestamp) {
        req->set_timestamp(ToProto<i64>(options.Timestamp));
    }

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();

    TQuery query;
    FromProto(&query, rsp->query());
    return query;
}

////////////////////////////////////////////////////////////////////////////////

TListQueriesResult TClient::DoListQueries(const TListQueriesOptions& options)
{
    TQueryTrackerServiceProxy proxy(
        Connection_->GetQueryTrackerChannelOrThrow(options.QueryTrackerStage));

    auto req = proxy.ListQueries();
    req->SetTimeout(options.Timeout);
    req->SetUser(*Options_.User);

    if (options.FromTime) {
        req->set_from_time(NYT::ToProto<i64>(*options.FromTime));
    }
    if (options.ToTime) {
        req->set_to_time(NYT::ToProto<i64>(*options.ToTime));
    }
    if (options.CursorTime) {
        req->set_cursor_time(NYT::ToProto<i64>(*options.CursorTime));
    }

    req->set_cursor_direction(static_cast<NProto::EOperationSortDirection>(options.CursorDirection));

    if (options.UserFilter) {
        req->set_user_filter(*options.UserFilter);
    }
    if (options.StateFilter) {
        req->set_state_filter(NProto::ConvertQueryStateToProto(*options.StateFilter));
    }
    if (options.EngineFilter) {
        req->set_engine_filter(NProto::ConvertQueryEngineToProto(*options.EngineFilter));
    }
    if (options.SubstrFilter) {
        req->set_substr_filter(*options.SubstrFilter);
    }

    req->set_limit(options.Limit);

    if (options.Attributes) {
        ToProto(req->mutable_attributes(), options.Attributes);
    }

    auto rsp = WaitFor(req->Invoke()).ValueOrThrow();

    std::vector<TQuery> queries(rsp->queries_size());
    for (auto i = 0; i < rsp->queries_size(); i++) {
        FromProto(&queries[i], rsp->queries()[i]);
    }
    return TListQueriesResult{
        .Queries = std::move(queries),
        .Incomplete = rsp->incomplete(),
        .Timestamp = rsp->timestamp(),
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

    ToProto(req->mutable_query_id(), queryId);
    if (options.Annotations) {
        req->set_annotations(ConvertToYsonString(options.Annotations).ToString());
    }
    if (options.AccessControlObject) {
        req->set_access_control_object(*options.AccessControlObject);
    }

    WaitFor(req->Invoke())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TGetQueryTrackerInfoResult TClient::DoGetQueryTrackerInfo(const TGetQueryTrackerInfoOptions& options)
{
    IClientPtr client;
    TString root;
    std::tie(client, root) = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    YT_LOG_DEBUG(
        "Getting query tracker information (Attributes: %v)",
        options.Attributes);

    auto attributes = options.Attributes;

    attributes.ValidateKeysOnly();

    TString clusterName = "";

    if (attributes.AdmitsKeySlow("cluster_name")) {
        YT_LOG_DEBUG("Getting cluster name");
        clusterName = client->GetClusterName().value_or("");
    }

    TNodeExistsOptions nodeExistsOptions;
    nodeExistsOptions.ReadFrom = EMasterChannelKind::Cache;
    bool accessControlSupported = WaitFor(client->NodeExists(TString(QueriesAcoNamespacePath), nodeExistsOptions))
        .ValueOrThrow();

    static const TYsonString EmptyMap = TYsonString(TString("{}"));
    TYsonString supportedFeatures = EmptyMap;
    if (attributes.AdmitsKeySlow("supported_features")) {
        supportedFeatures = BuildYsonStringFluently()
            .BeginMap()
                .Item("access_control").Value(accessControlSupported)
            .EndMap();
    }

    std::vector<TString> accessControlObjects;
    if (accessControlSupported && attributes.AdmitsKeySlow("access_control_objects")) {
        YT_LOG_DEBUG("Getting access control objects");
        TListNodeOptions listOptions;
        listOptions.ReadFrom = EMasterChannelKind::Cache;
        auto allAcos = WaitFor(client->ListNode(TString(QueriesAcoNamespacePath), listOptions))
            .ValueOrThrow();
        accessControlObjects = ConvertTo<std::vector<TString>>(allAcos);
    }

    return TGetQueryTrackerInfoResult{
        .ClusterName = std::move(clusterName),
        .SupportedFeatures = std::move(supportedFeatures),
        .AccessControlObjects = std::move(accessControlObjects),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
