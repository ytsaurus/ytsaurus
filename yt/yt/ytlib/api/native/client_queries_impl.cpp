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

namespace NDetail {

// Applies when a query doesn't have an access control object.
constexpr TStringBuf DefaultAccessControlObject = "nobody";

// Path to access control object namespace for QT.
constexpr TStringBuf QueriesAcoNamespacePath = "//sys/access_control_object_namespaces/queries";

TQuery PartialRecordToQuery(const auto& partialRecord)
{
    static_assert(pfr::tuple_size<TQuery>::value == 15);
    static_assert(TActiveQueryDescriptor::FieldCount == 19);
    static_assert(TFinishedQueryDescriptor::FieldCount == 14);

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
    query.AccessControlObject = partialRecord.AccessControlObject.value_or(std::nullopt);
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
        fillIfPresent("incarnation", partialRecord.Incarnation);
        fillIfPresent("lease_transaction_id", partialRecord.LeaseTransactionId);
        fillIfPresent("assigned_tracker", partialRecord.AssignedTracker);
    }

    query.OtherAttributes = std::move(otherAttributes);

    return query;
}

//! Lookup one of query tracker state tables by query id.
template <class TRecordDescriptor>
TFuture<typename TRecordDescriptor::TRecordPartial> LookupQueryTrackerRecord(
    TQueryId queryId,
    const IClientPtr& client,
    const TString& tablePath,
    const TString& tableKind,
    const std::optional<std::vector<TString>>& lookupKeys,
    NTransactionClient::TTimestamp timestamp)
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

THashSet<TString> GetUserSubjects(const TString& user, const IClientPtr& client)
{
    // Get all subjects for the user.
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    auto userSubjectsOrError = WaitFor(client->GetNode("//sys/users/" + user + "/@member_of_closure", options));
    if (!userSubjectsOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error while fetching user membership for the user %Qv", user)
            << userSubjectsOrError;
    }
    auto userSubjects = ConvertTo<THashSet<TString>>(userSubjectsOrError.Value());
    return userSubjects;
}

NSecurityClient::ESecurityAction CheckAccessControl(
    const TString& user,
    const std::optional<TString>& accessControlObject,
    const TString& queryAuthor,
    const IClientPtr& client,
    EPermission permission,
    const NLogging::TLogger& logger)
{
    auto& Logger = logger;
    if (user == queryAuthor) {
        return NSecurityClient::ESecurityAction::Allow;
    }
    auto actualAccessControlObject = accessControlObject.value_or(TString(DefaultAccessControlObject));
    auto aclOrError = WaitFor(client->GetNode(Format(
        "%v/%v/@principal_acl",
        QueriesAcoNamespacePath,
        NYPath::ToYPathLiteral(actualAccessControlObject))));
    if (!aclOrError.IsOK()) {
        YT_LOG_WARNING(aclOrError,
            "Error while fetching access control object queries/%v",
            actualAccessControlObject);
        auto userSubjects = GetUserSubjects(user, client);
        if (userSubjects.contains(NSecurityClient::SuperusersGroupName)) {
            return NSecurityClient::ESecurityAction::Allow;
        }
        THROW_ERROR_EXCEPTION(
            "Error while fetching access control object queries/%v. "
            "Please make sure it exists",
            actualAccessControlObject)
            << aclOrError;
    }
    return WaitFor(client->CheckPermissionByAcl(user, permission, ConvertToNode(aclOrError.Value())))
        .ValueOrThrow()
        .Action;
}

void ThrowAccessDeniedException(
    TQueryId queryId,
    EPermission permission,
    const TString& user,
    const std::optional<TString>& accessControlObject,
    const TString& queryAuthor)
{
    THROW_ERROR_EXCEPTION(NSecurityClient::EErrorCode::AuthorizationError,
        "Access denied to query %v due to missing %Qv permission",
        queryId,
        permission)
        << TErrorAttribute("User", user)
        << TErrorAttribute("Access control object", accessControlObject)
        << TErrorAttribute("Query author", queryAuthor);
}

//! Lookup a query in active_queries and finished_queries tables by query id.
TQuery LookupQuery(
    TQueryId queryId,
    const IClientPtr& client,
    const TString& root,
    const std::optional<std::vector<TString>>& lookupKeys,
    NTransactionClient::TTimestamp timestamp,
    const NLogging::TLogger& logger)
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
    NTransactionClient::TTimestamp timestamp,
    const TString& user,
    const IClientPtr& client,
    EPermission permission,
    const NLogging::TLogger& logger)
{
    std::vector<TString> lookupKeys = {"user", "access_control_object"};
    auto query = LookupQuery(queryId, client, root, lookupKeys, timestamp, logger);
    if (CheckAccessControl(user, query.AccessControlObject, *query.User, client, permission, logger) == NSecurityClient::ESecurityAction::Deny) {
        ThrowAccessDeniedException(queryId, permission, user, query.AccessControlObject, *query.User);
    }
}

std::vector<TString> GetAcosForSubjects(const THashSet<TString>& subjects, const IClientPtr& client)
{
    // Get all access control objects.
    TGetNodeOptions options;
    options.Attributes = {
        "principal_acl",
    };
    options.ReadFrom = EMasterChannelKind::Cache;

    auto allAcosOrError = WaitFor(client->GetNode(TString(QueriesAcoNamespacePath), options));

    if (!allAcosOrError.IsOK()) {
        THROW_ERROR_EXCEPTION(
            "Error while fetching all access control objects in the namespace \"queries\". "
            "Please make sure that the namespace exists")
            << allAcosOrError;
    }

    auto allAcos = ConvertToNode(allAcosOrError.Value())->AsMap()->GetChildren();

    std::vector<TString> acosForUser;
    // We expect average user to have access to a small number of access control objects.
    acosForUser.reserve(10);

    for (const auto& aco : allAcos) {
        auto acoName = aco.first;
        auto aclRules = ConvertToNode(aco.second->Attributes().GetYson("principal_acl"))->AsList()->GetChildren();
        bool allowUseRuleFound = false;
        bool denyUseRuleFound = false;
        // Check if there are allow or deny "Use" rules matching the subjects.
        for (const auto& aclRule : aclRules) {
            auto aclSubjects = aclRule->AsMap()->GetChildOrThrow("subjects")->AsList()->GetChildren();
            auto aclPermissions = aclRule->AsMap()->GetChildOrThrow("permissions")->AsList()->GetChildren();
            bool usePermissionFound = false;
            for (const auto& aclPermission : aclPermissions) {
                auto aclPermissionName = aclPermission->GetValue<TString>();
                aclPermissionName.to_lower();
                if (aclPermissionName == "use") {
                    usePermissionFound = true;
                    break;
                }
            }
            if (!usePermissionFound) {
                continue;
            }
            for (const auto& aclSubject : aclSubjects) {
                auto aclSubjectName = aclSubject->GetValue<TString>();
                if (subjects.find(aclSubjectName) != subjects.end()) {
                    auto aclAction = aclRule->AsMap()->GetChildOrThrow("action")->GetValue<TString>();
                    aclAction.to_lower();
                    if (aclAction == "allow") {
                        allowUseRuleFound = true;
                    } else if (aclAction == "deny") {
                        denyUseRuleFound = true;
                    }
                }
            }
        }
        if (allowUseRuleFound && !denyUseRuleFound) {
            acosForUser.emplace_back(acoName);
        }
    }
    return acosForUser;
}

void VerifyAccessControlObjectExists(const TString& accessControlObject, const IClientPtr& client)
{
    auto error = WaitFor(client->NodeExists(Format(
        "%v/%v",
        QueriesAcoNamespacePath,
        NYPath::ToYPathLiteral(accessControlObject))));

    if (!error.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to check whether access control object %Qv exists", accessControlObject)
            << error;
    }
    if (!error.Value()) {
        THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError,
            "Access control object %Qv does not exist",
            accessControlObject);
    }
}

////////////////////////////////////////////////////////////////////////////////

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

    auto [client, root] = GetNativeConnection()->GetQueryTrackerStage(options.QueryTrackerStage);

    static const TYsonString EmptyMap = TYsonString(TString("{}"));

    auto queryId = TQueryId::Create();

    YT_LOG_DEBUG("Starting query (QueryId: %v, Draft: %v)", queryId, options.Draft);

    auto rowBuffer = New<TRowBuffer>();
    auto transaction = WaitFor(client->StartTransaction(ETransactionType::Tablet, {}))
        .ValueOrThrow();

    if (options.AccessControlObject) {
        VerifyAccessControlObjectExists(*options.AccessControlObject, client);
    }

    // Draft queries go directly to finished query tables (regular and ordered by start time),
    // non-draft queries go to the active query table.

    if (options.Draft) {
        TString filterFactors;
        auto startTime = TInstant::Now();
        {
            static_assert(TFinishedQueryDescriptor::FieldCount == 14);
            TFinishedQuery newRecord{
                .Key = {.QueryId = queryId},
                .Engine = engine,
                .Query = query,
                .Files = ConvertToYsonString(options.Files),
                .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
                .User = *Options_.User,
                .AccessControlObject = options.AccessControlObject,
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
                .AccessControlObject = options.AccessControlObject,
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
        static_assert(TActiveQueryDescriptor::FieldCount == 19);
        TActiveQueryPartial newRecord{
            .Key = {.QueryId = queryId},
            .Engine = engine,
            .Query = query,
            .Files = ConvertToYsonString(options.Files),
            .Settings = options.Settings ? ConvertToYsonString(options.Settings) : EmptyMap,
            .User = *Options_.User,
            .AccessControlObject = options.AccessControlObject,
            .StartTime = TInstant::Now(),
            .State = EQueryState::Pending,
            .Incarnation = -1,
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

    ValidateQueryPermissions(queryId, root, transaction->GetStartTimestamp(), *Options_.User, client, EPermission::Administer, Logger);

    {
        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        TLookupRowsOptions options;
        options.Timestamp = transaction->GetStartTimestamp();
        options.ColumnFilter = {*idMapping.State};
        options.KeepMissingRows = true;
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

    auto timestamp = WaitFor(client->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    TQueryResult queryResult;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;

        ValidateQueryPermissions(queryId, root, timestamp, *Options_.User, client, EPermission::Read, Logger);

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

    auto timestamp = WaitFor(client->GetTimestampProvider()->GenerateTimestamps())
        .ValueOrThrow();

    TString wireRowset;
    TTableSchemaPtr schema;
    {
        auto rowBuffer = New<TRowBuffer>();
        TLookupRowsOptions options;
        options.KeepMissingRows = true;

        ValidateQueryPermissions(queryId, root, timestamp, *Options_.User, client, EPermission::Read, Logger);

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

    ValidateQueryPermissions(queryId, root, timestamp, *Options_.User, client, EPermission::Use, Logger);

    auto lookupKeys = options.Attributes ? std::make_optional(options.Attributes.Keys) : std::nullopt;

    return LookupQuery(queryId, client, root, lookupKeys, timestamp, Logger);
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

    auto userSubjects = GetUserSubjects(*Options_.User, client);
    userSubjects.insert(*Options_.User);

    std::vector<TString> userSubjectsVector(userSubjects.begin(), userSubjects.end());
    YT_LOG_DEBUG("Fetched user %Qv subjects: %v", *Options_.User, userSubjectsVector);

    bool isSuperuser = userSubjects.contains(NSecurityClient::SuperusersGroupName);
    std::vector<TString> acosForUser;

    if (!isSuperuser) {
        acosForUser = GetAcosForSubjects(userSubjects, client);
        YT_LOG_DEBUG("Fetched suitable access control objects for user %Qv: %v", *Options_.User, acosForUser);
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

    auto formatAcosInString = [] (const std::vector<TString>& accessControlObjects) {
        TStringBuilder builder;
        for (int index = 0; index < std::ssize(accessControlObjects); ++index) {
            builder.AppendString("\"");
            builder.AppendString(accessControlObjects[index]);
            builder.AppendString("\"");
            if (index + 1 != std::ssize(accessControlObjects)) {
                builder.AppendString(",");
            }
        }
        return builder.Flush();
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
        if (!isSuperuser) {
            if (acosForUser.empty()) {
                builder.AddWhereConjunct(Format("user = %Qv", *Options_.User));
            } else {
                builder.AddWhereConjunct(Format("user = %Qv OR access_control_object IN (%v)", *Options_.User, formatAcosInString(acosForUser)));
            }
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

    auto timestamp = transaction->GetStartTimestamp();

    ValidateQueryPermissions(queryId, root, timestamp, *Options_.User, client, EPermission::Administer, Logger);

    std::vector<TString> lookupKeys = {"state", "start_time"};

    auto query = LookupQuery(queryId, client, root, lookupKeys, timestamp, Logger);

    YT_LOG_DEBUG(
        "Altering query (QueryId: %v, State: %v, HasAnnotations: %v, HasAccessControlObject: %v)",
        queryId,
        *query.State,
        static_cast<bool>(options.Annotations),
        static_cast<bool>(options.AccessControlObject));

    if (!options.Annotations && !options.AccessControlObject) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
        return;
    }

    if (options.AccessControlObject) {
        VerifyAccessControlObjectExists(*options.AccessControlObject, client);
    }

    if (IsFinishedState(*query.State)) {
        auto rowBuffer = New<TRowBuffer>();
        TString filterFactors;
        {
            TFinishedQueryPartial record{
                .Key = {.QueryId = queryId},
            };
            if (options.Annotations) {
                record.Annotations = ConvertToYsonString(options.Annotations);
                filterFactors = GetFilterFactors(record);
            }
            if (options.AccessControlObject) {
                record.AccessControlObject = options.AccessControlObject;
            }
            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                root + "/finished_queries",
                TFinishedQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            TFinishedQueryByStartTimePartial record{
                .Key = {.StartTime = *query.StartTime, .QueryId = queryId},
            };
            if (options.Annotations) {
                record.FilterFactors = filterFactors;
            }
            if (options.AccessControlObject) {
                record.AccessControlObject = options.AccessControlObject;
            }
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
            };
            if (options.Annotations) {
                record.Annotations = ConvertToYsonString(options.Annotations);
                record.FilterFactors = GetFilterFactors(record);
            }
            if (options.AccessControlObject) {
                record.AccessControlObject = options.AccessControlObject;
            }
            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                root + "/active_queries",
                TActiveQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
    }

    WaitFor(transaction->Commit())
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
