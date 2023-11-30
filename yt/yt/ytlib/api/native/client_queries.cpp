#include "client_impl.h"
#include "config.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/query_tracker_client/config.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/connection.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/ytree/convert.h>

#include <contrib/libs/pfr/include/pfr/tuple_size.hpp>

#include <library/cpp/iterator/functools.h>

namespace NYT::NApi::NNative {

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

    auto [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    static const TYsonString EmptyMap = TYsonString(TString("{}"));

    auto queryId = TQueryId::Create();

    YT_LOG_DEBUG("Starting query (QueryId: %v, Draft: %v)", queryId, options.Draft);

    auto rowBuffer = New<TRowBuffer>();
    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    // Draft queries go directly to finished query tables (regular and ordered by start time),
    // non-draft queries go to the active query table.

    if (options.Draft) {
        TString filterFactors;
        auto startTime = TInstant::Now();
        {
            static_assert(TFinishedQueryDescriptor::FieldCount == 13);
            TFinishedQuery newRecord{
                .Key = {.QueryId = queryId},
                .Engine = engine,
                .Query = query,
                .Files = ConvertToYsonString(options.Files),
                .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
                .User = *Options_.User,
                .StartTime = startTime,
                .State = EQueryState::Draft,
                .Progress = EmptyMap,
                .Annotations = options.Annotations ? ConvertToYsonString(options.Annotations) : EmptyMap,
            };
            filterFactors = GetFilterFactors(newRecord);
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                root + "/finished_queries",
                TFinishedQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            TFinishedQueryByStartTime newRecord{
                .Key = {.StartTime = startTime, .QueryId = queryId},
                .Engine = engine,
                .User = *Options_.User,
                .State = EQueryState::Draft,
                .FilterFactors = filterFactors,
            };
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                root + "/finished_queries_by_start_time",
                TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
    } else {
        static_assert(TActiveQueryDescriptor::FieldCount == 18);
        TActiveQueryPartial newRecord{
            .Key = {.QueryId = queryId},
            .Engine = engine,
            .Query = query,
            .Files = ConvertToYsonString(options.Files),
            .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
            .User = *Options_.User,
            .StartTime = TInstant::Now(),
            .State = EQueryState::Pending,
            .Incarnation = -1,
            .PingTime = TInstant::Zero(),
            .Progress = EmptyMap,
            .Annotations = options.Annotations ? ConvertToYsonString(options.Annotations) : EmptyMap,
        };
        newRecord.FilterFactors = GetFilterFactors(newRecord);
        std::vector rows{
            newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
        };
        transaction->WriteRows(
            root + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(rows), rowBuffer));
    }
    WaitFor(transaction->Commit())
        .ThrowOnError();
    return queryId;
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAbortQuery(TQueryId queryId, const TAbortQueryOptions& options)
{
    auto [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    auto abortError = TError(
        "Query was aborted by user %Qv%v",
        Options_.User,
        options.AbortMessage ? Format(" with message %Qv", *options.AbortMessage) : TString());

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
            .ValueOrThrow()
            .Rowset;
        auto optionalRecords = ToOptionalRecords<TActiveQuery>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryNotFound,
                "Query %v not found or is not running",
                queryId);
        }
        const auto& record = *optionalRecords[0];
        if (record.State != EQueryState::Pending && record.State != EQueryState::Running) {
            THROW_ERROR_EXCEPTION("Cannot abort query %v which is in state %Qlv",
                queryId,
                record.State);
        }
    }

    transaction->WriteRows(
        root + "/active_queries",
        TActiveQueryDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    auto error = WaitFor(transaction->Commit());
    if (!error.IsOK()) {
        if (error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict)) {
            // TODO(max42): retry such errors automatically?
            THROW_ERROR_EXCEPTION("Cannot abort query because its state is being changed at the moment; please try again")
                << error;
        }
        THROW_ERROR error;
    }
}

////////////////////////////////////////////////////////////////////////////////

TQueryResult TClient::DoGetQueryResult(TQueryId queryId, i64 resultIndex, const TGetQueryResultOptions& options)
{
    auto [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    TQueryResult queryResult;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.EnablePartialResult = true;
        TFinishedQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = client->LookupRows(
            root + "/finished_query_results",
            TFinishedQueryResultDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow()
            .Rowset;
        auto optionalRecords = ToOptionalRecords<TFinishedQueryResult>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryResultNotFound,
                "Query %v result %v not found or is expired",
                queryId,
                resultIndex);
        }
        const auto& record = *optionalRecords[0];

        queryResult.Id = queryId;
        queryResult.ResultIndex = resultIndex;
        auto schemaNode = record.Schema ? ConvertToNode(*record.Schema) : nullptr;
        queryResult.Schema = schemaNode && schemaNode->GetType() == ENodeType::List ? ConvertTo<TTableSchemaPtr>(schemaNode) : nullptr;
        queryResult.Error = record.Error;
        queryResult.IsTruncated = record.IsTruncated;
        queryResult.DataStatistics = ConvertTo<TDataStatistics>(record.DataStatistics);
    }

    return queryResult;
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowsetPtr FilterRowsetColumns(IUnversionedRowsetPtr rowset, std::vector<TString> columns)
{
    const auto& schema = rowset->GetSchema();
    const auto& rows = rowset->GetRows();
    auto rowBuffer = New<TRowBuffer>();
    auto nameTable = TNameTable::FromSchema(*schema);
    std::vector<int> columnIndexes;
    THashSet<int> columnIndexSet;
    for (const auto& column : columns) {
        if (auto id = nameTable->FindId(column); id && columnIndexSet.insert(*id).second) {
            columnIndexes.push_back(*id);
        }
    }
    TColumnFilter columnFilter(columnIndexes);

    std::vector<TUnversionedRow> newRows;
    newRows.reserve(rows.size());
    for (const auto& row : rows) {
        auto newRow = rowBuffer->AllocateUnversioned(columnIndexes.size());
        for (auto [newIndex, index] : Enumerate(columnIndexes)) {
            auto value = row[index];
            value.Id = newIndex;
            newRow[newIndex] = rowBuffer->CaptureValue(value);
        }
        newRows.emplace_back(newRow);
    }

    auto newSchema = schema->Filter(columnFilter);

    return CreateRowset(newSchema, MakeSharedRange(std::move(newRows), std::move(rowBuffer)));
}

IUnversionedRowsetPtr TClient::DoReadQueryResult(TQueryId queryId, i64 resultIndex, const TReadQueryResultOptions& options)
{
    auto [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    YT_LOG_DEBUG(
        "Reading query result (QueryId: %v, ResultIndex: %v, LowerRowIndex: %v, UpperRowIndex: %v)",
        queryId,
        resultIndex,
        options.UpperRowIndex,
        options.UpperRowIndex);

    TString wireRowset;
    TTableSchemaPtr schema;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.EnablePartialResult = true;
        TFinishedQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = client->LookupRows(
            root + "/finished_query_results",
            TFinishedQueryResultDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), rowBuffer),
            options);
        auto rowset = WaitFor(asyncLookupResult)
            .ValueOrThrow()
            .Rowset;
        auto optionalRecords = ToOptionalRecords<TFinishedQueryResult>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryResultNotFound,
                "Query %v result %v not found or is expired",
                queryId,
                resultIndex);
        }
        const auto& record = *optionalRecords[0];
        if (!record.Error.IsOK()) {
            THROW_ERROR record.Error;
        }
        if (!record.Rowset) {
            // This should not normally happen, but better this than abort.
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::QueryResultNotFound,
                "Query %v result %v rowset is missing",
                queryId,
                resultIndex);
        }
        wireRowset = *record.Rowset;
        schema = ConvertTo<TTableSchemaPtr>(record.Schema);
    }

    auto wireReader = CreateWireProtocolReader(TSharedRef::FromString(std::move(wireRowset)));
    auto schemaData = IWireProtocolReader::GetSchemaData(*schema);
    auto rows = wireReader->ReadSchemafulRowset(schemaData, /*captureValues*/ true);

    {
        auto lowerRowIndex = options.LowerRowIndex.value_or(0);
        lowerRowIndex = std::clamp<i64>(lowerRowIndex, 0, rows.size());
        auto upperRowIndex = options.UpperRowIndex.value_or(rows.size());
        upperRowIndex = std::clamp<i64>(upperRowIndex, lowerRowIndex, rows.size());
        rows = rows.Slice(lowerRowIndex, upperRowIndex);
    }

    auto rowset = CreateRowset(std::move(schema), std::move(rows));

    if (options.Columns) {
        rowset = FilterRowsetColumns(rowset, *options.Columns);
    }

    return rowset;
}

////////////////////////////////////////////////////////////////////////////////

TQuery PartialRecordToQuery(const auto& partialRecord)
{
    static_assert(pfr::tuple_size<TQuery>::value == 14);
    static_assert(TActiveQueryDescriptor::FieldCount == 18);
    static_assert(TFinishedQueryDescriptor::FieldCount == 13);

    TQuery query;
    // Note that some of the fields are twice optional.
    // First time due to the fact that they are optional in the record,
    // and second time due to the extra optionality of any field in the partial record.
    // TODO(max42): coalesce the optionality of the fields in the partial record.
    query.Id = partialRecord.Key.QueryId;
    query.Engine = partialRecord.Engine;
    query.Query = partialRecord.Query;
    query.Files = partialRecord.Files.value_or(std::nullopt);
    query.StartTime = partialRecord.StartTime;
    query.FinishTime = partialRecord.FinishTime.value_or(std::nullopt);
    query.Settings = partialRecord.Settings.value_or(TYsonString());
    query.User = partialRecord.User;
    query.State = partialRecord.State;
    query.ResultCount = partialRecord.ResultCount.value_or(std::nullopt);
    query.Progress = partialRecord.Progress.value_or(TYsonString());
    query.Error = partialRecord.Error.value_or(std::nullopt);
    query.Annotations = partialRecord.Annotations.value_or(TYsonString());

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

    auto timestamp = options.Timestamp != NTransactionClient::NullTimestamp
        ? options.Timestamp
        : WaitFor(client->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();

    YT_LOG_DEBUG("Getting query (QueryId: %v, Timestamp: %v, Attributes: %v)", queryId, timestamp, options.Attributes);

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
        auto asyncRecord = asyncLookupResult.Apply(BIND([=] (const TUnversionedLookupRowsResult& result) {
            auto optionalRecords = ToOptionalRecords<typename TRecordDescriptor::TRecordPartial>(result.Rowset);
            YT_VERIFY(optionalRecords.size() == 1);
            if (!optionalRecords[0]) {
                THROW_ERROR_EXCEPTION("Query %v is not found in %Qv query table",
                    queryId,
                    tableKind);
            }
            return *optionalRecords[0];
        }));
        return asyncRecord;
    };

    auto asyncActiveRecord = lookupInTable.template operator()<TActiveQueryDescriptor>("/active_queries", "active");
    auto asyncFinishedRecord = lookupInTable.template operator()<TFinishedQueryDescriptor>("/finished_queries", "finished");

    auto error = WaitFor(AnySucceeded(std::vector{asyncActiveRecord.As<void>(), asyncFinishedRecord.As<void>()}));
    if (!error.IsOK()) {
        THROW_ERROR_EXCEPTION(
            NQueryTrackerClient::EErrorCode::QueryNotFound,
            "Query %v is not found neither in active nor in finished query tables",
            queryId)
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

    YT_LOG_DEBUG(
        "Listing queries (Timestamp: %v, State: %v, CursorDirection: %v, FromTime: %v, ToTime: %v, CursorTime: %v,"
        "Substr: %v, User: %v, Engine: %v, Limit: %v, Attributes: %v)",
        timestamp,
        options.StateFilter,
        options.CursorDirection,
        options.FromTime,
        options.ToTime,
        options.CursorTime,
        options.SubstrFilter,
        options.UserFilter,
        options.EngineFilter,
        options.Limit,
        options.Attributes);

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
            YT_LOG_DEBUG("Selecting active queries (Query: %v)", query);
            auto selectResult = WaitFor(client->SelectRows(query, options))
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
                YT_LOG_DEBUG("Selecting finished queries by start time (Query: %v)", query);
                auto selectResult = WaitFor(client->SelectRows(query, options))
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
                YT_LOG_DEBUG("Selecting admitted finished queries (Query: %v)", query);
                auto selectResult = WaitFor(client->SelectRows(query, options))
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
        std::optional<bool> stateFilterDefinesFinishedQuery = options.StateFilter
            ? std::make_optional(IsFinishedState(*options.StateFilter))
            : std::nullopt;
        if (!options.StateFilter || *stateFilterDefinesFinishedQuery) {
            futures.push_back(BIND(selectFinishedQueries).AsyncVia(GetCurrentInvoker()).Run());
        }
        if (!options.StateFilter || !*stateFilterDefinesFinishedQuery) {
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
                if (result.size() == options.Limit) {
                    // We are finishing collecting queries prematurely, set incomplete flag and break.
                    incomplete = true;
                    break;
                }
                result.push_back(std::move(query));
            }
        }
    } else {
        incomplete = options.Limit < queries.size();
        result = std::vector(queries.begin(), queries.end() + std::min(options.Limit, queries.size()));
    }

    return TListQueriesResult{
        .Queries = std::move(result),
        .Incomplete = incomplete,
        .Timestamp = timestamp,
    };
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoAlterQuery(TQueryId queryId, const TAlterQueryOptions& options)
{
    auto [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    auto query = WaitFor(GetQuery(
        queryId,
        TGetQueryOptions{
            .Attributes = {{"state", "start_time"}},
            .Timestamp = transaction->GetStartTimestamp()
        }))
        .ValueOrThrow();


    YT_LOG_DEBUG(
        "Altering query (QueryId: %v, State: %v, HasAnnotations: %v)",
        queryId,
        *query.State,
        static_cast<bool>(options.Annotations));

    if (options.Annotations) {
        if (IsFinishedState(*query.State)) {
            auto rowBuffer = New<TRowBuffer>();
            TString filterFactors;
            {
                TFinishedQueryPartial record{
                    .Key = {.QueryId = queryId},
                    .Annotations = ConvertToYsonString(options.Annotations),
                };
                std::vector rows{
                    record.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    root + "/finished_queries",
                    TFinishedQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
                filterFactors = GetFilterFactors(record);
            }
            {
                TFinishedQueryByStartTimePartial record{
                    .Key = {.StartTime = *query.StartTime, .QueryId = queryId},
                    .FilterFactors = filterFactors,
                };
                std::vector rows{
                    record.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    root + "/finished_queries_by_start_time",
                    TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        } else {
            auto rowBuffer = New<TRowBuffer>();
            {
                TActiveQueryPartial record{
                    .Key = {.QueryId = queryId},
                    .Annotations = ConvertToYsonString(options.Annotations),
                };
                record.FilterFactors = GetFilterFactors(record);
                std::vector rows{
                    record.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    root + "/active_queries",
                    TActiveQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
    }

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
