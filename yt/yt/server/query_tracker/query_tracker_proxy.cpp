#include "query_tracker_proxy.h"

#include "config.h"
#include "helpers.h"
#include "profiler.h"
#include "search_index.h"
#include "yql_engine.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/ytlib/yql_client/yql_service_proxy.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/record_helpers.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/iterator/functools.h>

#include <contrib/libs/pfr/include/pfr/tuple_size.hpp>

namespace NYT::NQueryTracker {

using namespace NApi;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger("QueryTrackerProxy");

namespace NDetail {

constexpr int MaxAccessControlObjectsPerQuery = 10;

// Path to access control object namespace for QT.
const NYPath::TYPath QueriesAcoNamespacePath = "//sys/access_control_object_namespaces/queries";

static const TYsonString EmptyMap = TYsonString(TString("{}"));
static const TString CompressedEmptyMap = Compress(EmptyMap.ToString(), MaxDyntableStringSize);

//! Lookup one of query tracker state tables by query id.
template <class TRecordDescriptor>
TFuture<typename TRecordDescriptor::TRecordPartial> LookupQueryTrackerRecord(
    TQueryId queryId,
    const IClientPtr& client,
    const TString& tablePath,
    const TString& tableKind,
    const std::optional<std::vector<std::string>>& lookupKeys,
    TTimestamp timestamp)
{
    auto rowBuffer = New<TRowBuffer>();
    const auto& nameTable = TRecordDescriptor::Get()->GetNameTable();

    TLookupRowsOptions lookupOptions;
    if (lookupKeys) {
        std::vector<int> columnIds;
        for (const auto& key : *lookupKeys) {
            if (auto columnId = nameTable->FindId(key)) {
                columnIds.push_back(*columnId);
            }
        }
        lookupOptions.ColumnFilter = TColumnFilter(columnIds);
    }
    lookupOptions.Timestamp = timestamp;
    lookupOptions.KeepMissingRows = true;
    std::vector keys{
        TActiveQueryKey{.QueryId = queryId}.ToKey(rowBuffer),
    };
    auto asyncLookupResult = client->LookupRows(
        tablePath,
        TRecordDescriptor::Get()->GetNameTable(),
        MakeSharedRange(std::move(keys), rowBuffer),
        lookupOptions);
    auto asyncRecord = asyncLookupResult.Apply(BIND([=] (const TUnversionedLookupRowsResult& result) {
        auto optionalRecords = ToOptionalRecords<typename TRecordDescriptor::TRecordPartial>(result.Rowset);
        YT_VERIFY(optionalRecords.size() == 1);
        if (!optionalRecords[0]) {
            THROW_ERROR_EXCEPTION("Query %v is not found in %Qv query table", queryId, tableKind);
        }
        return *optionalRecords[0];
    }));
    return asyncRecord;
};

ESecurityAction CheckAccessControl(
    const std::string& user,
    const std::optional<TYsonString>& accessControlObjects,
    const std::string& queryAuthor,
    const IClientPtr& client,
    EPermission permission)
{
    if (user == queryAuthor) {
        return ESecurityAction::Allow;
    }

    auto userSubjects = GetUserSubjects(user, client);
    if (userSubjects.contains(NSecurityClient::SuperusersGroupName)) {
        return NSecurityClient::ESecurityAction::Allow;
    }

    auto accessControlObjectList = ConvertTo<std::optional<std::vector<TString>>>(accessControlObjects);
    if (!accessControlObjectList) {
        return NSecurityClient::ESecurityAction::Deny;
    }

    TCheckPermissionOptions checkPermissionOptions;
    checkPermissionOptions.ReadFrom = EMasterChannelKind::Cache;
    checkPermissionOptions.SuccessStalenessBound = TDuration::Minutes(1);
    for (const auto& accessControlObject : *accessControlObjectList) {
        auto path = Format(
            "%v/%v/principal",
            QueriesAcoNamespacePath,
            NYPath::ToYPathLiteral(accessControlObject));

        auto securityAction = WaitFor(client->CheckPermission(user, path, permission, checkPermissionOptions))
            .ValueOrThrow()
            .Action;

        if (securityAction == NSecurityClient::ESecurityAction::Allow) {
            return NSecurityClient::ESecurityAction::Allow;
        }
    }

    return NSecurityClient::ESecurityAction::Deny;
}

void ThrowAccessDeniedException(
    TQueryId queryId,
    EPermission permission,
    const std::string& user,
    const std::optional<TYsonString>& accessControlObjects,
    const std::string& queryAuthor)
{
    THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AuthorizationError,
        "Access denied to query %v due to missing %Qv permission",
        queryId,
        permission)
        << TErrorAttribute("user", user)
        << TErrorAttribute("access_control_objects", accessControlObjects)
        << TErrorAttribute("query_author", queryAuthor);
}

//! Lookup a query in active_queries and finished_queries tables by query id.
TQuery LookupQuery(
    TQueryId queryId,
    const IClientPtr& client,
    const TString& root,
    const std::optional<std::vector<std::string>>& lookupKeys,
    TTimestamp timestamp,
    const TLogger& logger)
{
    auto asyncActiveRecord = LookupQueryTrackerRecord<TActiveQueryDescriptor>(
        queryId,
        client,
        root + "/active_queries",
        "active",
        lookupKeys,
        timestamp);
    auto asyncFinishedRecord = LookupQueryTrackerRecord<TFinishedQueryDescriptor>(
        queryId,
        client,
        root + "/finished_queries",
        "finished",
        lookupKeys,
        timestamp);

    auto error = WaitFor(AnySucceeded(std::vector{asyncActiveRecord.AsVoid(), asyncFinishedRecord.AsVoid()}));
    if (!error.IsOK()) {
        THROW_ERROR_EXCEPTION(NQueryTrackerClient::EErrorCode::QueryNotFound,
            "Query %v is not found neither in active nor in finished query tables",
            queryId)
            << error;
    }
    bool isActive = asyncActiveRecord.IsSet() && asyncActiveRecord.Get().IsOK();
    bool isFinished = asyncFinishedRecord.IsSet() && asyncFinishedRecord.Get().IsOK();
    YT_VERIFY(isActive || isFinished);
    if (isActive && isFinished) {
        const auto& Logger = logger;
        YT_LOG_ALERT(
            "Query is found in both active and finished query tables "
            "(QueryId: %v, Timestamp: %v)",
            queryId,
            timestamp);
    }
    if (isActive) {
        return PartialRecordToQuery(asyncActiveRecord.Get().Value());
    } else {
        return PartialRecordToQuery(asyncFinishedRecord.Get().Value());
    }
}

void ValidateQueryPermissions(
    TQueryId queryId,
    const TString& root,
    TTimestamp timestamp,
    const std::string& user,
    const IClientPtr& client,
    EPermission permission,
    const TLogger& logger)
{
    static const std::vector<std::string> lookupKeys{
        "user",
        "access_control_objects",
    };
    auto query = LookupQuery(queryId, client, root, lookupKeys, timestamp, logger);
    if (CheckAccessControl(user, query.AccessControlObjects, *query.User, client, permission) == ESecurityAction::Deny) {
        ThrowAccessDeniedException(queryId, permission, user, query.AccessControlObjects, *query.User);
    }
}

void VerifyAllAccessControlObjectsExist(const std::vector<std::string>& accessControlObjects, const IClientPtr& client)
{
    TNodeExistsOptions nodeExistsOptions;
    nodeExistsOptions.ReadFrom = EMasterChannelKind::Cache;
    nodeExistsOptions.SuccessStalenessBound = TDuration::Minutes(1);

    std::vector<TFuture<bool>> futures;
    for (const auto& accessControlObject : accessControlObjects) {
        futures.push_back(client->NodeExists(Format("%v/%v", QueriesAcoNamespacePath, ToYPathLiteral(accessControlObject)), nodeExistsOptions)
            .Apply(BIND([accessControlObject] (const TErrorOr<bool>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    THROW_ERROR_EXCEPTION("Failed to check whether access control object %Qv exists", accessControlObject)
                        << rspOrError;
                }

                if (!rspOrError.Value()) {
                    THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
                        "Access control object %Qv does not exist",
                        accessControlObject);
                }

                return true;
            })));
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
}

IUnversionedRowsetPtr FilterRowsetColumns(IUnversionedRowsetPtr rowset, std::vector<std::string> columns)
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

std::optional<std::vector<std::string>> ValidateAccessControlObjects(
    const std::optional<std::string>& accessControlObject,
    const std::optional<std::vector<std::string>>& accessControlObjects)
{
    if (!accessControlObjects && !accessControlObject) {
        return std::nullopt;
    }

    if (accessControlObjects && accessControlObject) {
        THROW_ERROR_EXCEPTION("Only one of \"access_control_objects\", \"access_control_object\" should be specified");
    }

    if (accessControlObject) {
        return std::vector<std::string>({*accessControlObject});
    }

    if (accessControlObjects->size() > MaxAccessControlObjectsPerQuery) {
        THROW_ERROR_EXCEPTION(NQueryTrackerClient::EErrorCode::TooManyAcos,
            "Too many ACOs in query: limit is %v, actual count is %v",
                MaxAccessControlObjectsPerQuery,
                accessControlObjects->size());
    }
    return accessControlObjects;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxy::TQueryTrackerProxy(
    IClientPtr stateClient,
    TYPath stateRoot,
    TQueryTrackerProxyConfigPtr config,
    int expectedTablesVersion)
    : StateClient_(std::move(stateClient))
    , StateRoot_(std::move(stateRoot))
    , ProxyConfig_(std::move(config))
    , ExpectedTablesVersion_(expectedTablesVersion)
    , TimeBasedIndex_(CreateTimeBasedIndex(StateClient_, StateRoot_))
    , TokenBasedIndex_(CreateTokenBasedIndex(StateClient_, StateRoot_))
{
    EngineProviders_[EQueryEngine::Yql] = CreateProxyYqlEngineProvider(StateClient_, StateRoot_);
}

void TQueryTrackerProxy::Reconfigure(const TQueryTrackerProxyConfigPtr& config, const TDuration notIndexedQueriesTTL)
{
    ProxyConfig_ = config;
    NotIndexedQueriesTTL_ = notIndexedQueriesTTL;
}

void TQueryTrackerProxy::StartQuery(
    const TQueryId queryId,
    const EQueryEngine engine,
    const TString& query,
    const TStartQueryOptions& options,
    const std::string& user)
{
    if (ssize(options.Files) > ProxyConfig_->MaxQueryFileCount) {
        THROW_ERROR_EXCEPTION("Too many files: limit is %v, actual count is %v",
            ProxyConfig_->MaxQueryFileCount,
            options.Files.size());
    }
    for (const auto& file : options.Files) {
        if (ssize(file->Name) > ProxyConfig_->MaxQueryFileNameSizeBytes) {
            THROW_ERROR_EXCEPTION("Too large file %v name: limit is %v, actual size is %v",
                file->Name,
                ProxyConfig_->MaxQueryFileNameSizeBytes,
                file->Name.size());
        }
        if (ssize(file->Content) > ProxyConfig_->MaxQueryFileContentSizeBytes) {
            THROW_ERROR_EXCEPTION("Too large file %v content: limit is %v, actual size is %v",
                file->Name,
                ProxyConfig_->MaxQueryFileContentSizeBytes,
                file->Content.size());
        }
    }

    auto isIndexed = options.Settings ? options.Settings->AsMap()->GetChildValueOrDefault("is_indexed", true) : true;

    YT_LOG_DEBUG("Starting query (QueryId: %v, Draft: %v, IsIndexed: %v)",
        queryId,
        options.Draft,
        isIndexed);

    auto rowBuffer = New<TRowBuffer>();
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    auto accessControlObjects = ValidateAccessControlObjects(options.AccessControlObject, options.AccessControlObjects).value_or(std::vector<std::string>{});
    VerifyAllAccessControlObjectsExist(accessControlObjects, StateClient_);

    auto annotations = options.Annotations ? ConvertToYsonString(options.Annotations) : EmptyMap;

    // Draft queries go directly to finished query tables (regular and ordered by start time),
    // non-draft queries go to the active query table.

    auto startTime = TInstant::Now();
    bool isTutorial = false;

    if (options.Annotations) {
        auto annotationsMap = options.Annotations->AsMap();
        if (annotationsMap->GetChildValueOrDefault("is_tutorial", false)) {
            if (!GetUserSubjects(user, StateClient_).contains(SuperusersGroupName)) {
                YT_LOG_DEBUG("Attempt to create a tutorial failed. User is not a superuser (User: %v)", user);
                THROW_ERROR_EXCEPTION("Non-superusers can't create tutorial queries. To create one contact your cluster administrator.");
            }
            if (!options.Draft) {
                THROW_ERROR_EXCEPTION("Tutorials should be in draft state.");
            }

            isTutorial = true;
        }
    }

    if (options.Draft) {
        TString filterFactors;
        {
            static_assert(TFinishedQueryDescriptor::FieldCount == 19);
            TFinishedQueryPartial newRecord{
                .Key = {.QueryId = queryId},
                .Engine = engine,
                .Query = query,
                .Files = ConvertToYsonString(options.Files),
                .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
                .User = user,
                .AccessControlObjects = ConvertToYsonString(accessControlObjects),
                .StartTime = startTime,
                .State = EQueryState::Draft,
                .Progress = CompressedEmptyMap,
                .Annotations = annotations,
                .Secrets = ConvertToYsonString(options.Secrets),
                .IsIndexed = isIndexed,
                .IsTutorial = isTutorial,
            };
            if (!isIndexed) {
                newRecord.TTL = NotIndexedQueriesTTL_.MilliSeconds();
            }

            filterFactors = GetFilterFactors(newRecord);
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetPartialIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries",
                TFinishedQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));

            auto query = PartialRecordToQuery(newRecord);
            if (isIndexed) {
                TimeBasedIndex_->AddQuery(query, transaction);
                if (!isTutorial) {
                    TokenBasedIndex_->AddQuery(query, transaction);
                }
            }
        }
    } else {
        static_assert(TActiveQueryDescriptor::FieldCount == 23);

        TActiveQueryPartial newRecord{
            .Key = {.QueryId = queryId},
            .Engine = engine,
            .Query = query,
            .Files = ConvertToYsonString(options.Files),
            .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
            .User = user,
            .AccessControlObjects = ConvertToYsonString(accessControlObjects),
            .StartTime = startTime,
            .State = EQueryState::Pending,
            .Incarnation = -1,
            .Progress = CompressedEmptyMap,
            .Annotations = annotations,
            .Secrets = ConvertToYsonString(options.Secrets),
            .IsIndexed = isIndexed,
            .IsTutorial = isTutorial,
        };
        newRecord.FilterFactors = GetFilterFactors(newRecord);
        std::vector rows{
            newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetPartialIdMapping()),
        };
        transaction->WriteRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(rows), rowBuffer));

        auto query = PartialRecordToQuery(newRecord);
        if (isIndexed) {
            TimeBasedIndex_->AddQuery(query, transaction);
            YT_VERIFY(!isTutorial);
            TokenBasedIndex_->AddQuery(query, transaction);
        }
    }

    // Forces the use of two-phase commit protocol.
    // Disables single-phase optimization due to an issue (YT-25550).
    TTransactionCommitOptions commitOptions{
        .Force2PC = true,
    };

    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();
}

void TQueryTrackerProxy::AbortQuery(
    const TQueryId queryId,
    const TAbortQueryOptions& options,
    const std::string& user)
{
    auto abortError = TError(
        "Query was aborted by user %Qv%v",
        user,
        options.AbortMessage ? Format(" with message %Qv", *options.AbortMessage) : TString());

    TActiveQueryPartial newRecord{
        .Key = {.QueryId = queryId},
        .State = EQueryState::Aborting,
        .FinishTime = TInstant::Now(),
        .AbortRequest = ConvertToYsonString(abortError),
    };
    auto rowBuffer = New<TRowBuffer>();
    std::vector rows{
        newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetPartialIdMapping()),
    };
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    ValidateQueryPermissions(queryId, StateRoot_, transaction->GetStartTimestamp(), user, StateClient_, EPermission::Administer, Logger);

    YT_LOG_DEBUG("Aborting query (QueryId: %v, AbortMessage: %v)", queryId, options.AbortMessage);

    TActiveQuery record;
    {
        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        TLookupRowsOptions options;
        options.Timestamp = transaction->GetStartTimestamp();
        options.ColumnFilter = {idMapping.State, idMapping.Engine, idMapping.AssignedTracker, idMapping.ExecutionStartTime};
        options.KeepMissingRows = true;
        TActiveQueryKey key{.QueryId = queryId};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/active_queries",
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
        record = *optionalRecords[0];
        if (record.State != EQueryState::Pending && record.State != EQueryState::Running) {
            THROW_ERROR_EXCEPTION("Cannot abort query %v which is in state %Qlv",
                queryId,
                record.State);
        }
    }

    transaction->WriteRows(
        StateRoot_ + "/active_queries",
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

    {
        // Save profile counter.
        auto now = TInstant::Now();
        auto time = now - record.StartTime;
        if (record.State == EQueryState::Running) {
            time = now - *record.ExecutionStartTime;
        }

        auto& stateTimeGauge = GetOrCreateProfilingCounter<TStateTimeProfilingCounter>(
            QueryTrackerProfiler,
            ProfilingTagsFromActiveQueryRecord(record))->StateTime;
        stateTimeGauge.Update(time);
    }
}

TQueryResult TQueryTrackerProxy::GetQueryResult(
    const TQueryId queryId,
    const i64 resultIndex,
    const std::string& user)
{
    auto timestamp = WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    YT_LOG_DEBUG("Getting query result (QueryId: %v, ResultIndex: %v)", queryId, resultIndex);

    TQueryResult queryResult;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;

        ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Read, Logger);

        TFinishedQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/finished_query_results",
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
        queryResult.FullResult = record.FullResult ? *record.FullResult : NYson::TYsonString();
        queryResult.DataStatistics = ConvertTo<TDataStatistics>(record.DataStatistics);
    }

    return queryResult;
}

IUnversionedRowsetPtr TQueryTrackerProxy::ReadQueryResult(
    const TQueryId queryId,
    const i64 resultIndex,
    const TReadQueryResultOptions& options,
    const std::string& user)
{
    YT_LOG_DEBUG(
        "Reading query result (QueryId: %v, ResultIndex: %v, LowerRowIndex: %v, UpperRowIndex: %v)",
        queryId,
        resultIndex,
        options.LowerRowIndex,
        options.UpperRowIndex);

    auto timestamp = WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    TString wireRowset;
    TTableSchemaPtr schema;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;

        ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Read, Logger);

        TFinishedQueryResultKey key{.QueryId = queryId, .Index = resultIndex};
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/finished_query_results",
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

TQuery TQueryTrackerProxy::GetQuery(
    const TQueryId queryId,
    const TGetQueryOptions& options,
    const std::string& user)
{
    auto timestamp = options.Timestamp != NullTimestamp
        ? options.Timestamp
        : WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
            .ValueOrThrow();

    YT_LOG_DEBUG("Getting query (QueryId: %v, Timestamp: %v, Attributes: %v)",
        queryId,
        timestamp,
        options.Attributes);

    options.Attributes.ValidateKeysOnly();

    ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Use, Logger);

    auto lookupKeys = options.Attributes ? std::make_optional(options.Attributes.Keys()) : std::nullopt;

    auto query = LookupQuery(queryId, StateClient_, StateRoot_, lookupKeys, timestamp, Logger);

    if (!lookupKeys || std::find(lookupKeys->begin(), lookupKeys->end(), "access_control_object") != lookupKeys->end()) {
        ConvertAcoToOldFormat(query);
    }

    return query;
}

TListQueriesResult TQueryTrackerProxy::ListQueries(
    const TListQueriesOptions& options,
    const std::string& user)
{
    YT_LOG_DEBUG(
        "Listing queries (State: %v, CursorDirection: %v, FromTime: %v, ToTime: %v, CursorTime: %v,"
        "Substr: %v, User: %v, Engine: %v, Limit: %v, Attributes: %v, TutorialFilter: %v, SearchByTokenPrefix: %v, UseFullTextSearch: %v)",
        options.StateFilter,
        options.CursorDirection,
        options.FromTime,
        options.ToTime,
        options.CursorTime,
        options.SubstrFilter,
        options.UserFilter,
        options.EngineFilter,
        options.Limit,
        options.Attributes,
        options.TutorialFilter,
        options.SearchByTokenPrefix,
        options.UseFullTextSearch);

    if (options.SubstrFilter && !options.SubstrFilter->empty() && options.UseFullTextSearch) {
        return TokenBasedIndex_->ListQueries(options, user);
    } else {
        return TimeBasedIndex_->ListQueries(options, user);
    }
}

void TQueryTrackerProxy::AlterQuery(
    const TQueryId queryId,
    const TAlterQueryOptions& options,
    const std::string& user)
{
    auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    auto timestamp = transaction->GetStartTimestamp();

    ValidateQueryPermissions(queryId, StateRoot_, timestamp, user, StateClient_, EPermission::Administer, Logger);

    static const std::vector<std::string> lookupKeys{
        "query_id",
        "state",
        "start_time",
        "finish_time",
        "user",
        "query",
        "files",
        "access_control_objects",
        "engine",
        "settings",
        "annotations",
        "is_indexed",
        "is_tutorial",
    };

    auto query = LookupQuery(queryId, StateClient_, StateRoot_, lookupKeys, timestamp, Logger);
    auto isTutorial = query.OtherAttributes ? query.OtherAttributes->Get("is_tutorial", false) : false;
    auto isTutorialChanged = false;

    auto accessControlObjects = ValidateAccessControlObjects(options.AccessControlObject, options.AccessControlObjects);

    if (options.Annotations) {
        auto annotationsMap = options.Annotations->AsMap();
        if (annotationsMap->GetChildValueOrDefault("is_tutorial", false) != isTutorial) {
            if (!GetUserSubjects(user, StateClient_).contains(SuperusersGroupName)) {
                THROW_ERROR_EXCEPTION("Non-superusers can't change tutorial flag. To change one contact your cluster administrator.");
            }
            if (*query.State != EQueryState::Draft) {
                THROW_ERROR_EXCEPTION("Tutorials should be in draft state.");
            }

            isTutorialChanged = true;
            isTutorial = !isTutorial;
        }
    }

    YT_LOG_DEBUG(
        "Altering query (QueryId: %v, State: %v, HasAnnotations: %v, HasAccessControlObjects: %v)",
        queryId,
        *query.State,
        static_cast<bool>(options.Annotations),
        static_cast<bool>(accessControlObjects));

    if (!options.Annotations && !accessControlObjects) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
        return;
    }

    if (accessControlObjects) {
        VerifyAllAccessControlObjectsExist(*accessControlObjects, StateClient_);
    }

    if (IsFinishedState(*query.State)) {
        auto rowBuffer = New<TRowBuffer>();
        {
            TFinishedQueryPartial record{
                .Key = {.QueryId = queryId},
                .AccessControlObjects = query.AccessControlObjects ? query.AccessControlObjects : TYsonString(TString("[]")),
                .Annotations = query.Annotations ? query.Annotations : EmptyMap,
            };
            if (options.Annotations) {
                record.Annotations = ConvertToYsonString(options.Annotations);
            }
            if (accessControlObjects) {
                record.AccessControlObjects = ConvertToYsonString(accessControlObjects);
            }
            record.IsTutorial = isTutorial;

            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetPartialIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries",
                TFinishedQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));

            if (query.IsIndexed && query.IsIndexed.value()) {
                TUpdateQueryOptions indexUpdateQueryOptions{
                    .NewAnnotations = options.Annotations,
                    .NewAccessControlObjects = accessControlObjects
                };

                if (isTutorialChanged) {
                    // isTutorial is key column in all indexes, so we should remove all previous entries
                    TimeBasedIndex_->RemoveQuery(query, transaction);
                    if (isTutorial) {
                        TokenBasedIndex_->RemoveQuery(query, transaction);
                    }

                    if (accessControlObjects) {
                        query.AccessControlObjects = ConvertToYsonString(accessControlObjects);
                    }
                    if (options.Annotations) {
                        query.Annotations = ConvertToYsonString(options.Annotations);
                    }
                    if (!query.OtherAttributes) {
                        query.OtherAttributes = {};
                    }
                    query.OtherAttributes->Set("is_tutorial", isTutorial);

                    TimeBasedIndex_->AddQuery(query, transaction);
                    if (!isTutorial) {
                        TokenBasedIndex_->AddQuery(query, transaction);
                    }
                } else {
                    TimeBasedIndex_->UpdateQuery(
                        query,
                        indexUpdateQueryOptions,
                        transaction
                    );

                    if (!isTutorial) {
                        TokenBasedIndex_->UpdateQuery(
                            query,
                            indexUpdateQueryOptions,
                            transaction
                        );
                    }
                }
            }
        }
    } else {
        auto rowBuffer = New<TRowBuffer>();
        {
            TActiveQueryPartial record{
                .Key = {.QueryId = queryId},
                .Query = query.Query,
                .AccessControlObjects = query.AccessControlObjects ? query.AccessControlObjects : TYsonString(TString("[]")),
                .Annotations = query.Annotations ? query.Annotations : EmptyMap,
            };
            if (options.Annotations) {
                record.Annotations = ConvertToYsonString(options.Annotations);
            }
            if (accessControlObjects) {
                record.AccessControlObjects = ConvertToYsonString(accessControlObjects);
            }
            record.FilterFactors = GetFilterFactors(record);

            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetPartialIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/active_queries",
                TActiveQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));

            TUpdateQueryOptions indexUpdateOptions{
                .NewAnnotations = options.Annotations,
                .NewAccessControlObjects = accessControlObjects
            };

            TimeBasedIndex_->UpdateQuery(
                query,
                indexUpdateOptions,
                transaction
            );

            YT_VERIFY(!isTutorialChanged);
            if (!isTutorial) {
                TokenBasedIndex_->UpdateQuery(
                    query,
                    indexUpdateOptions,
                    transaction
                );
            }
        }
    }

    // Forces the use of two-phase commit protocol.
    // Disables single-phase optimization due to an issue (YT-25550).
    TTransactionCommitOptions commitOptions{
        .Force2PC = true,
    };

    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();
}

TGetQueryTrackerInfoResult TQueryTrackerProxy::GetQueryTrackerInfo(
    const TGetQueryTrackerInfoOptions& options)
{
    YT_LOG_DEBUG(
        "Getting query tracker information (Attributes: %v)",
        options.Attributes);

    auto settingsMap = options.Settings ? options.Settings->AsMap() : ConvertToNode(EmptyMap)->AsMap();

    auto attributes = options.Attributes;

    attributes.ValidateKeysOnly();

    std::string clusterName = "";

    if (attributes.AdmitsKeySlow("cluster_name")) {
        YT_LOG_DEBUG("Getting cluster name");
        clusterName = WaitFor(StateClient_->GetClusterName())
            .ValueOrDefault("")
            .value_or("");
    }

    TNodeExistsOptions nodeExistsOptions;
    nodeExistsOptions.ReadFrom = EMasterChannelKind::Cache;
    nodeExistsOptions.SuccessStalenessBound = TDuration::Minutes(1);

    TYsonString supportedFeatures = EmptyMap;
    if (attributes.AdmitsKeySlow("supported_features")) {
        // These features are guaranteed to be deployed before or with this code.
        supportedFeatures = BuildYsonStringFluently()
            .BeginMap()
                .Item("access_control").Value(true)
                .Item("multiple_aco").Value(true)
                .Item("new_search").Value(true)
                .Item("not_indexing").Value(true)
                .Item("declare_params").Value(true)
                .Item("tutorials").Value(true)
            .EndMap();
    }

    std::vector<std::string> accessControlObjects;
    if (attributes.AdmitsKeySlow("access_control_objects")) {
        YT_LOG_DEBUG("Getting access control objects");
        TListNodeOptions listOptions;
        listOptions.ReadFrom = EMasterChannelKind::Cache;
        listOptions.SuccessStalenessBound = TDuration::Minutes(1);
        auto allAcos = WaitFor(StateClient_->ListNode(QueriesAcoNamespacePath, listOptions))
            .ValueOrThrow();
        accessControlObjects = ConvertTo<std::vector<std::string>>(allAcos);
    }

    std::vector<std::string> clusters;
    if (attributes.AdmitsKeySlow("clusters")) {
        YT_LOG_DEBUG("Getting list of available clusters");
        TListNodeOptions listOptions;
        listOptions.ReadFrom = EMasterChannelKind::Cache;
        listOptions.SuccessStalenessBound = TDuration::Minutes(1);
        auto clustersYson = WaitFor(StateClient_->ListNode("//sys/clusters", listOptions))
            .ValueOrThrow();
        clusters = ConvertTo<std::vector<std::string>>(clustersYson);
    }

    auto enginesInfoMap = ConvertToNode(EmptyMap)->AsMap();
    if (attributes.AdmitsKeySlow("engines_info")) {
        try {
            auto yqlEngineInfo = EngineProviders_.contains(EQueryEngine::Yql) ? EngineProviders_[EQueryEngine::Yql]->GetEngineInfo(settingsMap) : EmptyMap;
            enginesInfoMap->AddChild("yql", ConvertToNode(yqlEngineInfo));
        } catch (const std::exception& ex) {
            YT_LOG_ERROR("GetEngineInfo call failed with exception. (Exception: %v)", ex);
        }
    }

    auto enginesInfo = ConvertToYsonString(enginesInfoMap);

    return TGetQueryTrackerInfoResult{
        .QueryTrackerStage = options.QueryTrackerStage,
        .ClusterName = std::move(clusterName),
        .SupportedFeatures = std::move(supportedFeatures),
        .AccessControlObjects = std::move(accessControlObjects),
        .Clusters = std::move(clusters),
        .EnginesInfo = std::move(enginesInfo),
        .ExpectedTablesVersion = ExpectedTablesVersion_,
    };
}

TGetQueryDeclaredParametersInfoResult TQueryTrackerProxy::GetQueryDeclaredParametersInfo(
    const TGetQueryDeclaredParametersInfoOptions& options)
{
    static const TYsonString EmptyMap = TYsonString(TString("{}"));
    auto parameters = EngineProviders_.contains(options.Engine) ? EngineProviders_[options.Engine]->GetDeclaredParametersInfo(options.Query, options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap) : EmptyMap;
    return TGetQueryDeclaredParametersInfoResult{
        .Parameters = parameters,
    };
}


////////////////////////////////////////////////////////////////////////////////

TQueryTrackerProxyPtr CreateQueryTrackerProxy(
    IClientPtr stateClient,
    TYPath stateRoot,
    TQueryTrackerProxyConfigPtr config,
    int expectedTablesVersion)
{
    return New<TQueryTrackerProxy>(
        std::move(stateClient),
        std::move(stateRoot),
        std::move(config),
        expectedTablesVersion);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
