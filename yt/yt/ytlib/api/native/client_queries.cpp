#include "client_impl.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <contrib/libs/pfr/include/pfr/tuple_size.hpp>

namespace NYT::NApi::NNative {

using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TQueryId TClient::DoStartQuery(EQueryEngine engine, const TString& query, const TStartQueryOptions& options)
{
    const auto& [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    static const TYsonString EmptyMap = TYsonString(TString("{}"));

    auto queryId = TQueryId::Create();
    TActiveQueryPartial newRecord{
        .Key = {.QueryId = queryId},
        .Engine = engine,
        .Query = query,
        .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
        .User = Options_.User,
        .StartTime = TInstant::Now(),
        .State = EQueryState::Pending,
        .Incarnation = -1,
        .PingTime = TInstant::Zero(),
        .Progress = EmptyMap,
    };
    newRecord.FilterFactors = GetFilterFactors(newRecord);
    auto rowBuffer = New<TRowBuffer>();
    std::vector rows{
        newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
    };
    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();
    transaction->WriteRows(
        root + "/active_queries",
        TActiveQueryDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));
    WaitFor(transaction->Commit())
        .ThrowOnError();
    return queryId;
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAbortQuery(TQueryId queryId, const TAbortQueryOptions& options)
{
    const auto& [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    auto abortError = TError(
        "Query was aborted by user %Qv%v",
        Options_.User,
        options.AbortMessage ? " with message \"" + *options.AbortMessage + "\"" : "");

    TActiveQueryPartial newRecord{
        .Key = {.QueryId = queryId},
        .State = EQueryState::Aborting,
        .FinishTime = TInstant::Now(),
        .AbortRequest = ConvertToYsonString(abortError),
    };
    auto rowBuffer = New<TRowBuffer>();
    std::vector rows{
        newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
    };
    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    {
        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        TLookupRowsOptions options;
        options.Timestamp = transaction->GetStartTimestamp();
        options.ColumnFilter = {*idMapping.State};
        options.EnablePartialResult = true;
        TActiveQueryKey key{.QueryId = queryId};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = client->LookupRows(
            root + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow();
        auto optionalRecords = ToOptionalRecords<TActiveQuery>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION("Query %Qv not found or is not running", queryId);
        }
        const auto& record = *optionalRecords[0];
        if (record.State != EQueryState::Pending && record.State != EQueryState::Running) {
            THROW_ERROR_EXCEPTION("Cannot abort query %Qv which is in state %Qlv", queryId, record.State);
        }
    }

    transaction->WriteRows(
        root + "/active_queries",
        TActiveQueryDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    auto error = WaitFor(transaction->Commit());
    if (error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict)) {
        // TODO(max42): retry such errors automatically?
        THROW_ERROR_EXCEPTION("Cannot abort query because its state is being changed at the moment; please try again")
            << error;
    } else {
        THROW_ERROR error;
    }
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowsetPtr TClient::DoReadQueryResult(TQueryId queryId, i64 resultIndex, const TReadQueryResultOptions& options)
{
    const auto& [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    TString wireRowset;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.EnablePartialResult = true;
        TQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = client->LookupRows(
            root + "/query_results",
            TQueryResultDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow();
        auto optionalRecords = ToOptionalRecords<TQueryResult>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION("Query %Qv result %v not found or is expired", queryId, resultIndex);
        }
        const auto& record = *optionalRecords[0];
        wireRowset = record.Rowset;
    }

    auto wireReader = CreateWireProtocolReader(TSharedRef::FromString(std::move(wireRowset)));
    auto schema = wireReader->ReadTableSchema();
    auto schemaData = IWireProtocolReader::GetSchemaData(schema);
    auto rows = wireReader->ReadSchemafulRowset(schemaData, /*captureValues*/ true);
    return CreateRowset(New<TTableSchema>(std::move(schema)), std::move(rows));
}

////////////////////////////////////////////////////////////////////////////////

TQuery PartialRecordToQuery(const auto& partialRecord)
{
    static_assert(pfr::tuple_size<TQuery>::value == 12);
    static_assert(TActiveQueryDescriptor::FieldCount == 16);
    static_assert(TFinishedQueryDescriptor::FieldCount == 11);

    TQuery query;
    // Note that some of the fields are twice optional.
    // First time due to the fact that they are optional in the record,
    // and second time due to the extra optionality of any field in the partial record.
    // TODO(max42): coalesce the optionality of the fields in the partial record.
    query.Id = partialRecord.Key.QueryId;
    query.Engine = partialRecord.Engine;
    query.Query = partialRecord.Query;
    query.StartTime = partialRecord.StartTime;
    query.FinishTime = partialRecord.FinishTime.value_or(std::nullopt);
    query.Settings = partialRecord.Settings.value_or(TYsonString());
    query.User = partialRecord.User;
    query.State = partialRecord.State;
    query.ResultCount = partialRecord.ResultCount.value_or(std::nullopt);
    query.Progress = partialRecord.Progress.value_or(TYsonString());
    query.Error = partialRecord.Error.value_or(std::nullopt);

    IAttributeDictionaryPtr otherAttributes;
    auto fillIfPresent = [&] (const TString& key, const auto& value) {
        if (value) {
            if (!otherAttributes) {
                otherAttributes = CreateEphemeralAttributes();
            }
            otherAttributes->Set(key, *value);
        }
    };

    if constexpr (std::is_same_v<std::decay_t<decltype(partialRecord)>, TActiveQueryPartial>) {
        fillIfPresent("abort_request", partialRecord.AbortRequest.value_or(std::nullopt));
        fillIfPresent("ping_time", partialRecord.PingTime);
        fillIfPresent("incarnation", partialRecord.Incarnation);
        fillIfPresent("assigned_tracker", partialRecord.AssignedTracker);
    }

    query.OtherAttributes = std::move(otherAttributes);

    return query;
}

std::vector<TQuery> PartialRecordsToQueries(const auto& partialRecords)
{
    std::vector<TQuery> queries;
    queries.reserve(partialRecords.size());
    for (const auto& partialRecord : partialRecords) {
        queries.push_back(PartialRecordToQuery(partialRecord));
    }
    return queries;
}

TQuery TClient::DoGetQuery(TQueryId queryId, const TGetQueryOptions& options)
{
    IClientPtr client;
    TString root;
    std::tie(client, root) = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    auto timestamp = WaitFor(client->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    options.Attributes.ValidateKeysOnly();
    auto rowBuffer = New<TRowBuffer>();

    auto lookupInTable = [=]<class TRecordDescriptor> (const TString& tableName, const TString& tableKind) {
        const auto& nameTable = TRecordDescriptor::Get()->GetNameTable();

        TLookupRowsOptions lookupOptions;
        if (options.Attributes) {
            std::vector<int> columnIds;
            for (const auto& key : options.Attributes.Keys) {
                if (auto columnId = nameTable->FindId(key)) {
                    columnIds.push_back(*columnId);
                }
            }
            lookupOptions.ColumnFilter = TColumnFilter(columnIds);
        }
        lookupOptions.Timestamp = timestamp;
        lookupOptions.EnablePartialResult = true;
        std::vector keys{
            TActiveQueryKey{.QueryId = queryId}.ToKey(rowBuffer),
        };
        auto asyncLookupResult = client->LookupRows(
            root + tableName,
            TRecordDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            lookupOptions);
        auto asyncRecord = asyncLookupResult.Apply(BIND([=] (const IUnversionedRowsetPtr& rowset) {
            auto optionalRecords = ToOptionalRecords<typename TRecordDescriptor::TRecordPartial>(rowset);
            YT_VERIFY(optionalRecords.size() == 1);
            if (!optionalRecords[0]) {
                THROW_ERROR_EXCEPTION("Query %Qv is not found in %Qv query table", queryId, tableKind);
            }
            return *optionalRecords[0];
        }));
        return asyncRecord;
    };

    auto asyncActiveRecord = lookupInTable.template operator()<TActiveQueryDescriptor>("/active_queries", "active");
    auto asyncFinishedRecord = lookupInTable.template operator()<TFinishedQueryDescriptor>("/finished_queries", "finished");

    auto error = WaitFor(AnySucceeded(std::vector{asyncActiveRecord.As<void>(), asyncFinishedRecord.As<void>()}));
    if (!error.IsOK()) {
        THROW_ERROR_EXCEPTION("Query %Qv is not found neither in active nor in finished query tables", queryId)
            << error;
    }
    bool isActive = asyncActiveRecord.IsSet() && asyncActiveRecord.Get().IsOK();
    bool isFinished = asyncFinishedRecord.IsSet() && asyncFinishedRecord.Get().IsOK();
    YT_VERIFY(isActive || isFinished);
    if (isActive && isFinished) {
        YT_LOG_ALERT("Query is found in both active and finished query tables (QueryId: %v, Timestamp: %v)", queryId, timestamp);
    }
    TQuery result;
    if (isActive) {
        result = PartialRecordToQuery(asyncActiveRecord.Get().Value());
    } else {
        result = PartialRecordToQuery(asyncFinishedRecord.Get().Value());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TListQueriesResult TClient::DoListQueries(const TListQueriesOptions& options)
{
    IClientPtr client;
    TString root;
    std::tie(client, root) = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    auto timestamp = WaitFor(client->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    auto attributes = options.Attributes;

    attributes.ValidateKeysOnly();

    if (!attributes.AdmitsKeySlow("start_time")) {
        YT_VERIFY(attributes);
        attributes.Keys.push_back("start_time");
    }

    auto addSelectExpressionsFromAttributes = [&] (TQueryBuilder& builder, const TNameTablePtr& nameTable) {
        if (attributes) {
            for (const auto& key : attributes.Keys) {
                if (nameTable->FindId(key)) {
                    builder.AddSelectExpression("[" + key + "]");
                }
            }
        } else {
            builder.AddSelectExpression("*");
        }
    };

    auto addSelectExpressionsForMerging = [&] (TQueryBuilder& builder) {
        if (!attributes.AdmitsKeySlow("query_id")) {
            builder.AddSelectExpression("[query_id]");
        }
    };

    auto addFilterConditions = [&] (TQueryBuilder& builder) {
        if (options.UserFilter) {
            builder.AddWhereConjunct("[user] = \"" + *options.UserFilter + "\"");
        }
        if (options.EngineFilter) {
            builder.AddWhereConjunct("[engine] = \"" + FormatEnum(*options.EngineFilter) + "\"");
        }
        if (options.StateFilter) {
            builder.AddWhereConjunct("[state] = \"" + FormatEnum(*options.StateFilter) + "\"");
        }
        if (options.FromTime) {
            builder.AddWhereConjunct("[start_time] >= " + ToString(options.FromTime->MicroSeconds()));
        }
        if (options.ToTime) {
            builder.AddWhereConjunct("[start_time] <= " + ToString(options.ToTime->MicroSeconds()));
        }
        if (options.SubstrFilter) {
            builder.AddWhereConjunct(Format("is_substr(%Qv, filter_factors)", *options.SubstrFilter));
        }
    };

    auto selectActiveQueries = [=, this, this_ = MakeStrong(this)] {
        try {
            TQueryBuilder builder;
            builder.SetSource(root + "/active_queries");
            addFilterConditions(builder);
            addSelectExpressionsFromAttributes(builder, TActiveQueryDescriptor::Get()->GetNameTable());
            addSelectExpressionsForMerging(builder);
            TSelectRowsOptions options;
            options.Timestamp = timestamp;
            auto query = builder.Build();
            YT_LOG_DEBUG("Selecting active queries (Query: %Qv)", query);
            auto selectResult = WaitFor(client->SelectRows(query))
                .ValueOrThrow();
            auto records = ToRecords<TActiveQueryPartial>(selectResult.Rowset);
            return PartialRecordsToQueries(records);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting active queries")
                << ex;
        }
    };

    auto selectFinishedQueries = [=, this, this_ = MakeStrong(this)] () -> std::vector<TQuery> {
        try {
            TStringBuilder admittedQueryIds;
            {
                TQueryBuilder builder;
                builder.SetSource(root + "/finished_queries_by_start_time");
                addFilterConditions(builder);
                builder.AddSelectExpression("[query_id]");
                TSelectRowsOptions options;
                options.Timestamp = timestamp;
                auto query = builder.Build();
                YT_LOG_DEBUG("Selecting finished queries by date (Query: %Qv)", query);
                auto selectResult = WaitFor(client->SelectRows(query))
                    .ValueOrThrow();
                bool isFirst = true;
                for (const auto& record : ToRecords<TFinishedQueryByStartTime>(selectResult.Rowset)) {
                    if (!isFirst) {
                        admittedQueryIds.AppendString(", ");
                    }
                    admittedQueryIds.AppendFormat("\"%v\"", record.Key.QueryId);
                    isFirst = false;
                }
                if (isFirst) {
                    return {};
                }
            }
            {
                TQueryBuilder builder;
                builder.SetSource(root + "/finished_queries");
                addSelectExpressionsFromAttributes(builder, TFinishedQueryDescriptor::Get()->GetNameTable());
                builder.AddWhereConjunct("[query_id] in (" + admittedQueryIds.Flush() + ")");
                addSelectExpressionsForMerging(builder);
                TSelectRowsOptions options;
                options.Timestamp = timestamp;
                auto query = builder.Build();
                YT_LOG_DEBUG("Selecting admitted finished queries (Query: %Qv)", query);
                auto selectResult = WaitFor(client->SelectRows(query))
                    .ValueOrThrow();
                auto records = ToRecords<TFinishedQueryPartial>(selectResult.Rowset);
                return PartialRecordsToQueries(records);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting finished queries")
                << ex;
        }
    };

    std::vector<TQuery> queries;
    {
        std::vector<TFuture<std::vector<TQuery>>> futures;
        bool stateFilterDefinesFinishedQuery =
            options.StateFilter == EQueryState::Completed || options.StateFilter == EQueryState::Failed || options.StateFilter == EQueryState::Aborted;
        if (!options.StateFilter || stateFilterDefinesFinishedQuery) {
            futures.push_back(BIND(selectFinishedQueries).AsyncVia(GetCurrentInvoker()).Run());
        }
        if (!options.StateFilter || !stateFilterDefinesFinishedQuery) {
            futures.push_back(BIND(selectActiveQueries).AsyncVia(GetCurrentInvoker()).Run());
        }
        auto queryVectors = WaitFor(AllSucceeded(futures))
            .ValueOrThrow();
        for (const auto& queryVector : queryVectors) {
            queries.insert(queries.end(), queryVector.begin(), queryVector.end());
        }
    }

    std::vector<TQuery> result;
    bool incomplete = false;
    if (options.CursorDirection != EOperationSortDirection::None) {
        auto compare = [&] (const TQuery& lhs, const TQuery& rhs) {
            return options.CursorDirection == EOperationSortDirection::Past
                ? std::tie(lhs.StartTime, lhs.Query) > std::tie(rhs.StartTime, rhs.Query)
                : std::tie(lhs.StartTime, lhs.Query) < std::tie(rhs.StartTime, rhs.Query);
        };
        std::sort(queries.begin(), queries.end(), compare);
        for (auto& query : queries) {
            if (!options.CursorTime ||
                (options.CursorDirection == EOperationSortDirection::Past && query.StartTime < options.CursorTime) ||
                (options.CursorDirection == EOperationSortDirection::Future && query.StartTime > options.CursorTime))
            {
                result.push_back(std::move(query));
            }
            if (result.size() == options.Limit) {
                break;
            }
        }
    } else {
        result = std::vector(queries.begin(), queries.end() + std::min(options.Limit, queries.size()));
    }

    if (result.size() < queries.size()) {
        incomplete = true;
    }

    return TListQueriesResult{
        .Queries = std::move(result),
        .Incomplete = incomplete,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
