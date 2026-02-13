#include "search_index.h"

#include "helpers.h"
#include "tokenizer.h"

#include <yt/yt/ytlib/query_tracker_client/helpers.h>
#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

namespace NYT::NQueryTracker {

using namespace NQueryTrackerClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NApi;
using namespace NYPath;
using namespace NHiveClient;
using namespace NYTree;
using namespace NRpc;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NQueryClient;
using namespace NYson;
using namespace NConcurrency;
using namespace NSecurityClient;

using TMinusTimestamp = i64;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger Logger("SearchIndex");
static const TYsonString EmptyMap = TYsonString(TString("{}"));
static const ui64 ShrunkFormattableViewLimit = 20;

const TString FinishedQueriesByAcoAndStartTimeTable = "finished_queries_by_aco_and_start_time";
const TString FinishedQueriesByUserAndStartTimeTable = "finished_queries_by_user_and_start_time";
const std::string SearchIndexTable = "search_inverted_index";
const std::string SearchMetaTable = "search_meta";

// Path to access control object namespace for QT.
const TYPath QueriesAcoNamespacePath = "//sys/access_control_object_namespaces/queries";

// Rows in SearchTable and SearchMetaTable are duplicated:
// one with the actual engine value, and one with NoEngineFilterString in the 'engine' column.
// This allows efficient searching both with and without an engine filter.
const std::string NoEngineFilterString = "";
// In SearchTable or SearchMetaTable rows which AccessScope belongs to the ACO are duplicated:
// one with the real user name, and one with NoUserFilterString in the 'user' column.
// This is used to save the request autor
// and is necessary to perform a quick search both with and without a user filter.
const std::string NoUserFilterString = "";
// Each row in SearchTable and SearchMetaTable which AccessScope starts with UserPrefix
// has a duplicate with AccessScope = SuperuserSearchAccessScope.
// This enables superusers to search across all queries when no user filter is applied.
const std::string SuperuserSearchAccessScope = "su";

// This ACO is the same as all others, but does not affect ListQueries. This is needed to enable link sharing.
constexpr TStringBuf EveryoneShareAccessControlObject = "everyone-share";

const std::string UserPrefix = "user:";
const std::string AcoPrefix = "aco:";

const std::unordered_set<std::string> UnindexedAnnotationKeys{"chartConfig", "assigned_engine"};

constexpr i64 TopRarestTokenLimit = 5;

////////////////////////////////////////////////////////////////////////////////

struct TQueryIndexSearchOptions
{
    std::string User;
    std::vector<std::string> AcosForUser;
    ui64 Timestamp;
    bool IsSuperuser;
    TAttributeFilter Attributes;
};

struct TSearchMetaRecordKeyComparator
{
    static_assert(TSearchMetaRecordDescriptor::FieldCount == 6);

    bool operator()(const TSearchMetaRecordKey& lhs, const TSearchMetaRecordKey& rhs) const
    {
        if (lhs.AccessScope != rhs.AccessScope) {
            return lhs.AccessScope < rhs.AccessScope;
        }
        if (lhs.Token != rhs.Token) {
            return lhs.Token < rhs.Token;
        }
        if (lhs.Engine != rhs.Engine) {
            return lhs.Engine < rhs.Engine;
        }
        return lhs.User < rhs.User;
    }
};

struct TSortOrderComparator
{
    EListQueriesSortOrder SortOrder;

    TSortOrderComparator(EListQueriesSortOrder sortOrder)
        : SortOrder(sortOrder)
    { }

    bool operator()(const TQuery& lhs, const TQuery& rhs)
    {
        return SortOrder == EListQueriesSortOrder::Descending
            ? std::tie(lhs.StartTime, lhs.Query) > std::tie(rhs.StartTime, rhs.Query)
            : std::tie(lhs.StartTime, lhs.Query) < std::tie(rhs.StartTime, rhs.Query);
    };
};

struct TCursorComparator
{
    EOperationSortDirection Cursor;

    TCursorComparator(EOperationSortDirection cursor)
        : Cursor(cursor)
    { }

    bool operator()(const TQuery& lhs, const TQuery& rhs)
    {
        return Cursor == EOperationSortDirection::Past
            ? std::tie(lhs.StartTime, lhs.Query) > std::tie(rhs.StartTime, rhs.Query)
            : std::tie(lhs.StartTime, lhs.Query) < std::tie(rhs.StartTime, rhs.Query);
    };
};

////////////////////////////////////////////////////////////////////////////////

std::pair<std::vector<std::string>, std::vector<std::string>> GetAccessControlDiff(
    const std::vector<std::string>& before,
    const std::optional<std::vector<std::string>>& after)
{
    std::vector<std::string> toDelete, toInsert;
    if (!after) {
        return {toDelete, toInsert};
    }

    THashSet<std::string> beforeSet(before.begin(), before.end());
    THashSet<std::string> afterSet(after->begin(), after->end());

    for (const auto& aco : before) {
        if (!afterSet.contains(aco)) {
            toDelete.push_back(aco);
        }
    }
    for (const auto& aco : *after) {
        if (!beforeSet.contains(aco)) {
            toInsert.push_back(aco);
        }
    }

    return {toDelete, toInsert};
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

void ValidateQuery(const TQuery& query)
{
    if (!query.User) {
        THROW_ERROR_EXCEPTION("User is lost in query %v", query.Id);
    } else if (!query.Query) {
        THROW_ERROR_EXCEPTION("QueryText is lost in query %v", query.Id);
    } else if (!query.StartTime) {
        THROW_ERROR_EXCEPTION("StartTime is lost in query %v", query.Id);
    } else if (!query.Engine) {
        THROW_ERROR_EXCEPTION("Engine is lost in query %v", query.Engine);
    } else if (!query.Annotations) {
        THROW_ERROR_EXCEPTION("Annotations are lost in query %v", query.Engine);
    } else if (!query.State) {
        THROW_ERROR_EXCEPTION("State is lost in query %v", query.Id);
    }
}

template <typename TLambdaResult>
std::vector<TLambdaResult> RunAsyncAndConcatenate(const std::vector<TExtendedCallback<std::vector<TLambdaResult>()>>& callbacks)
{
    if (callbacks.empty()) {
        return {};
    }
    std::vector<TFuture<std::vector<TLambdaResult>>> futures;
    for (const auto& callback : callbacks) {
        futures.push_back(callback.AsyncVia(GetCurrentInvoker()).Run());
    }

    auto resultVectors = WaitFor(AllSucceeded(futures))
        .ValueOrThrow();

    for (size_t i = 1; i < resultVectors.size(); ++i) {
        for (auto& resultVectorElement : resultVectors[i]) {
            resultVectors[0].push_back(std::move(resultVectorElement));
        }
    }

    return resultVectors[0];
}

void AddSelectExpressionsFromAttributes(TQueryBuilder& builder, const TNameTablePtr& nameTable, const TAttributeFilter& attributes)
{
    if (attributes) {
        for (const auto& key : attributes.Keys()) {
            if (nameTable->FindId(key)) {
                builder.AddSelectExpression("[" + key + "]");
            }
        }
    } else {
        builder.AddSelectExpression("*");
    }
};

void AddSelectExpressionsForMerging(TQueryBuilder& builder, const TAttributeFilter& attributes)
{
    if (!attributes.AdmitsKeySlow("query_id")) {
        builder.AddSelectExpression("[query_id]");
    }
};

std::vector<std::string> GetAccessScopes(
    const std::string& userName,
    const std::vector<std::string>& accessControlObjects)
{
    auto accessScopes = accessControlObjects;
    accessScopes.erase(std::remove(accessScopes.begin(), accessScopes.end(), "nobody"), accessScopes.end());
    for (auto& accessScope : accessScopes) {
        accessScope = AcoPrefix + accessScope;
    }
    accessScopes.push_back(UserPrefix + userName);
    accessScopes.push_back(SuperuserSearchAccessScope);
    return accessScopes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

using namespace NDetail;

////////////////////////////////////////////////////////////////////////////////

class TSearchIndexBase
    : public ISearchIndex
{
public:
    TSearchIndexBase(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
    { }

protected:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;

    TQueryIndexSearchOptions MakeQueryIndexSearchOptions(const TListQueriesOptions& options, const std::string& user)
    {
        auto keys = options.Attributes.Keys();
        TAttributeFilter attributes;

        options.Attributes.ValidateKeysOnly();

        if (!options.Attributes.AdmitsKeySlow("start_time")) {
            YT_VERIFY(options.Attributes);
            keys.push_back("start_time");
        }
        if (options.Attributes) {
            attributes = TAttributeFilter(std::move(keys));
        }

        auto userSubjects = GetUserSubjects(user, StateClient_);
        userSubjects.insert(user);

        YT_LOG_DEBUG(
            "Fetched user subjects (User: %v, Subjects: %v)",
            user,
            MakeShrunkFormattableView(userSubjects, TDefaultFormatter(), ShrunkFormattableViewLimit));

        bool isSuperuser = userSubjects.contains(SuperusersGroupName);
        std::vector<std::string> acosForUser;

        if (!isSuperuser) {
            acosForUser = GetAcosForSubjects(userSubjects, /*filterEveryoneShareAco*/ true);
            YT_LOG_DEBUG(
                "Fetched suitable access control objects for user (User: %v, Acos: %v)",
                user,
                MakeShrunkFormattableView(acosForUser, TDefaultFormatter(), ShrunkFormattableViewLimit));
        }

        return TQueryIndexSearchOptions{
            .User = user,
            .AcosForUser = std::move(acosForUser),
            .Timestamp = WaitFor(StateClient_->GetTimestampProvider()->GenerateTimestamps())
                .ValueOrThrow(),
            .IsSuperuser = isSuperuser,
            .Attributes = std::move(attributes),
        };
    }

    std::vector<TQuery> FetchQueriesByIds(
        const std::vector<TQueryId>& selectedQueryIds,
        const TListQueriesOptions& options,
        const TQueryIndexSearchOptions& indexSearchOptions)
    {
        auto placeholderValuesMap = BuildYsonStringFluently().BeginMap();
        std::vector<std::string> admittedQueryIds;
        for (const auto& selectedQueryId : selectedQueryIds) {
            admittedQueryIds.push_back(Format("%v", selectedQueryId));
        }

        TQueryBuilder builder;
        AddSelectExpressionsFromAttributes(builder, TFinishedQueryDescriptor::Get()->GetNameTable(), indexSearchOptions.Attributes);
        AddSelectExpressionsForMerging(builder, indexSearchOptions.Attributes);

        builder.AddWhereConjunct("[query_id] in {QueryIds}");
        placeholderValuesMap.Item("QueryIds").Value(admittedQueryIds);

        TSelectRowsOptions selectOptions;
        selectOptions.Timestamp = indexSearchOptions.Timestamp;
        selectOptions.PlaceholderValues = placeholderValuesMap.EndMap();

        std::vector<TExtendedCallback<std::vector<TQuery>()>> callbacks;
        builder.SetSource(StateRoot_ + "/finished_queries");
        auto query = builder.Build();
        callbacks.push_back(BIND([query, selectOptions, this]() {
            YT_LOG_DEBUG("Selecting admitted queries (Query: %v)", query);
            auto selectResult = WaitFor(StateClient_->SelectRows(query, selectOptions))
                .ValueOrThrow();
            auto records = ToRecords<TFinishedQueryPartial>(selectResult.Rowset);
            return PartialRecordsToQueries(records);
        }));

        builder.SetSource(StateRoot_ + "/active_queries");
        query = builder.Build();
        callbacks.push_back(BIND([query, selectOptions, this]() {
            YT_LOG_DEBUG("Selecting admitted queries (Query: %v)", query);
            auto selectResult = WaitFor(StateClient_->SelectRows(query, selectOptions))
                .ValueOrThrow();
            auto records = ToRecords<TActiveQueryPartial>(selectResult.Rowset);
            return PartialRecordsToQueries(records);
        }));

        auto queries = RunAsyncAndConcatenate<TQuery>(callbacks);

        if (options.SortOrder != EListQueriesSortOrder::Cursor) {
            std::sort(queries.begin(), queries.end(), TSortOrderComparator(options.SortOrder));
        } else {
            std::sort(queries.begin(), queries.end(), TCursorComparator(options.CursorDirection));
        }
        queries.erase(std::unique(queries.begin(), queries.end(), [&](const TQuery& lhs, const TQuery& rhs) { return lhs.Id == rhs.Id; }), queries.end());

        if (!indexSearchOptions.Attributes ||
            std::find(indexSearchOptions.Attributes.Keys().begin(), indexSearchOptions.Attributes.Keys().end(), "access_control_object") != indexSearchOptions.Attributes.Keys().end())
        {
            for (auto& query : queries) {
                ConvertAcoToOldFormat(query);
            }
        }

        return queries;
    }

private:
    std::vector<std::string> GetAcosForSubjects(
        const THashSet<std::string>& subjects,
        bool filterEveryoneShareAco)
    {
        // Get all access control objects.
        TListNodeOptions options;
        options.Attributes = {
            "principal_acl",
        };
        options.ReadFrom = EMasterChannelKind::Cache;
        options.SuccessStalenessBound = TDuration::Minutes(1);

        auto allAcosOrError = WaitFor(StateClient_->ListNode(TString(QueriesAcoNamespacePath), options));

        if (!allAcosOrError.IsOK()) {
            THROW_ERROR_EXCEPTION(
                "Error while fetching all access control objects in the namespace \"queries\"; "
                "please make sure that the namespace exists")
                << allAcosOrError;
        }

        auto allAcos = ConvertToNode(allAcosOrError.Value())->AsList()->GetChildren();

        std::vector<std::string> acosForUser;
        // We expect average user to have access to a small number of access control objects.
        acosForUser.reserve(10);

        for (const auto& acoNode : allAcos) {
            auto acoName = acoNode->AsString()->GetValue();
            if (filterEveryoneShareAco && acoName == EveryoneShareAccessControlObject) {
                continue;
            }

            auto aclRuleNodes = ConvertToNode(acoNode->Attributes().GetYson("principal_acl"))->AsList()->GetChildren();
            bool allowUseRuleFound = false;
            bool denyUseRuleFound = false;
            // Check if there are allow or deny "use" rules matching the subjects.
            for (const auto& aclRuleNode : aclRuleNodes) {
                auto aclSubjectNodes = aclRuleNode->AsMap()->GetChildOrThrow("subjects")->AsList()->GetChildren();
                auto aclPermissionNodes = aclRuleNode->AsMap()->GetChildOrThrow("permissions")->AsList()->GetChildren();
                bool usePermissionFound = false;
                for (const auto& aclPermissionNode : aclPermissionNodes) {
                    auto aclPermission = aclPermissionNode->GetValue<EPermission>();
                    if (aclPermission == EPermission::Use) {
                        usePermissionFound = true;
                        break;
                    }
                }
                if (!usePermissionFound) {
                    continue;
                }
                for (const auto& aclSubjectNode : aclSubjectNodes) {
                    auto aclSubjectName = aclSubjectNode->GetValue<std::string>();
                    if (subjects.find(aclSubjectName) != subjects.end()) {
                        auto aclAction = aclRuleNode->AsMap()->GetChildOrThrow("action")->GetValue<ESecurityAction>();
                        if (aclAction == ESecurityAction::Allow) {
                            allowUseRuleFound = true;
                        } else if (aclAction == ESecurityAction::Deny) {
                            denyUseRuleFound = true;
                        }
                    }
                }
            }
            if (allowUseRuleFound && !denyUseRuleFound) {
                acosForUser.push_back(acoName);
            }
        }
        return acosForUser;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTimeBasedIndex
    : public TSearchIndexBase
{
public:
    TTimeBasedIndex(IClientPtr stateClient, TYPath stateRoot)
        : TSearchIndexBase(std::move(stateClient), std::move(stateRoot))
    { }

    void AddQuery(const TQuery& query, ITransactionPtr transaction) override
    {
        ValidateQuery(query);

        // TimeBasedIndex contains only finished queries.
        if (!IsFinishedState(query.State.value())) {
            YT_LOG_DEBUG("Active query passed to the time-based index will not be stored (QueryId: %v)", query.Id);
            return;
        }
        auto rowBuffer = New<TRowBuffer>();

        auto filterFactors = TString(GetFilterFactors(query));
        auto isTutorial = query.OtherAttributes ? query.OtherAttributes->Get("is_tutorial", false) : false;

        auto accessControlObjects = query.AccessControlObjects ? ConvertTo<std::vector<std::string>>(query.AccessControlObjects) : std::vector<std::string>{};

        {
            static_assert(TFinishedQueryByStartTimeDescriptor::FieldCount == 8);

            TFinishedQueryByStartTime newRecord{
                .Key = {.IsTutorial = isTutorial, .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id},
                .Engine = query.Engine.value(),
                .User = query.User.value(),
                .AccessControlObjects = query.AccessControlObjects.value_or(EmptyMap),
                .State = query.State.value(),
                .FilterFactors = filterFactors,
            };
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetPartialIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries_by_start_time",
                TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            static_assert(TFinishedQueryByUserAndStartTimeDescriptor::FieldCount == 7);

            TFinishedQueryByUserAndStartTime newRecord{
                .Key = {.IsTutorial = isTutorial, .User = query.User.value(), .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id},
                .Engine = query.Engine.value(),
                .State = query.State.value(),
                .FilterFactors = filterFactors,
            };
            std::vector rows{
                newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetPartialIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/" + FinishedQueriesByUserAndStartTimeTable,
                TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            static_assert(TFinishedQueryByAcoAndStartTimeDescriptor::FieldCount == 8);

            if (!accessControlObjects.empty()) {
                std::vector<TUnversionedRow> rows;
                rows.reserve(accessControlObjects.size());
                for (const auto& aco : accessControlObjects) {
                    TFinishedQueryByAcoAndStartTime newRecord{
                        .Key = {.IsTutorial = isTutorial, .AccessControlObject = aco, .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id},
                        .Engine = query.Engine.value(),
                        .User = query.User.value(),
                        .State = query.State.value(),
                        .FilterFactors = filterFactors,
                    };
                    rows.push_back(newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetPartialIdMapping()));
                }
                transaction->WriteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
    }

    void RemoveQuery(const TQuery& query, ITransactionPtr transaction) override
    {
        ValidateQuery(query);

        // TimeBasedIndex contains only finished queries.
        if (!IsFinishedState(query.State.value())) {
            YT_LOG_DEBUG("Active query passed to the time-based index will not be deleted (QueryId: %v)", query.Id);
            return;
        }
        auto rowBuffer = New<TRowBuffer>();

        auto filterFactors = TString(GetFilterFactors(query));
        auto isTutorial = query.OtherAttributes ? query.OtherAttributes->Get("is_tutorial", false) : false;

        auto accessControlObjects = query.AccessControlObjects ? ConvertTo<std::vector<std::string>>(query.AccessControlObjects) : std::vector<std::string>{};

        {
            static_assert(TFinishedQueryByStartTimeDescriptor::FieldCount == 8);

            TFinishedQueryByStartTimeKey removeKey{
                .IsTutorial = isTutorial,
                .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()),
                .QueryId = query.Id
            };
            auto keys = FromRecordKeys(TRange(std::array{removeKey}));

            transaction->DeleteRows(
                StateRoot_ + "/finished_queries_by_start_time",
                TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                keys);
        }
        {
            static_assert(TFinishedQueryByUserAndStartTimeDescriptor::FieldCount == 7);

            TFinishedQueryByUserAndStartTimeKey removeKey{
                .IsTutorial = isTutorial,
                .User = query.User.value(),
                .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()),
                .QueryId = query.Id
            };
            auto keys = FromRecordKeys(TRange(std::array{removeKey}));

            transaction->DeleteRows(
                StateRoot_ + "/" + FinishedQueriesByUserAndStartTimeTable,
                TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetNameTable(),
                keys);
        }
        {
            static_assert(TFinishedQueryByAcoAndStartTimeDescriptor::FieldCount == 8);

            if (!accessControlObjects.empty()) {
                std::vector<TFinishedQueryByAcoAndStartTimeKey> keys;
                keys.reserve(accessControlObjects.size());
                for (const auto& aco : accessControlObjects) {
                    TFinishedQueryByAcoAndStartTimeKey removeKey{
                        .IsTutorial = isTutorial,
                        .AccessControlObject = aco,
                        .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()),
                        .QueryId = query.Id
                    };
                    keys.push_back(removeKey);
                }
                transaction->DeleteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    FromRecordKeys(TRange(keys)));
            }
        }
    }

    void UpdateQuery(const TQuery& query, const TUpdateQueryOptions& options, ITransactionPtr transaction) override
    {
        ValidateQuery(query);

        // TimeBasedIndex contains only finished queries.
        if (!IsFinishedState(query.State.value())) {
            YT_LOG_DEBUG("Active query passed to the time-based index will not be updated (QueryId: %v)", query.Id);
            return;
        }

        auto rowBuffer = New<TRowBuffer>();
        auto filterFactors = TString(BuildFilterFactors(
            query.Query.value(),
            options.NewAnnotations ? ConvertToYsonString(options.NewAnnotations, EYsonFormat::Text) : TYsonString(),
            options.NewAccessControlObjects ? ConvertToYsonString(options.NewAccessControlObjects) : TYsonString()));
        auto isTutorial = query.OtherAttributes ? query.OtherAttributes->Get("is_tutorial", false) : false;

        {
            TFinishedQueryByStartTimePartial record{
                .Key = {.IsTutorial = isTutorial, .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id},
                .FilterFactors = filterFactors,
            };
            if (options.NewAccessControlObjects) {
                record.AccessControlObjects = ConvertToYsonString(options.NewAccessControlObjects.value());
            }

            std::vector rows{
                record.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetPartialIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/finished_queries_by_start_time",
                TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(rows), rowBuffer));
        }
        {
            if (options.NewAnnotations) {
                TFinishedQueryByUserAndStartTimePartial record{
                    .Key = {.IsTutorial = isTutorial, .User = *query.User, .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id},
                    .FilterFactors = filterFactors,
                };

                std::vector rows{
                    record.ToUnversionedRow(rowBuffer, TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetPartialIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/" + FinishedQueriesByUserAndStartTimeTable,
                    TFinishedQueryByUserAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
        {
            auto previousAccessControlObjects = query.AccessControlObjects && *query.AccessControlObjects
                ? ConvertTo<std::vector<std::string>>(*query.AccessControlObjects)
                : std::vector<std::string>{};

            auto [acoToDelete, acoToInsert] = GetAccessControlDiff(previousAccessControlObjects, options.NewAccessControlObjects);

            if (!acoToDelete.empty()) {
                std::vector<TUnversionedRow> keysToDelete;
                keysToDelete.reserve(acoToDelete.size());
                for (const auto& aco : acoToDelete) {
                    keysToDelete.push_back(
                        TFinishedQueryByAcoAndStartTimeKey{.IsTutorial = isTutorial, .AccessControlObject = aco, .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id}.ToKey(rowBuffer));
                }
                transaction->DeleteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(keysToDelete), rowBuffer));
            }

            if (options.NewAnnotations) {
                auto newAccessControlObjects = options.NewAccessControlObjects.value_or(previousAccessControlObjects);
                acoToInsert = newAccessControlObjects;
            }
            if (!acoToInsert.empty()) {
                std::vector<TUnversionedRow> rows;
                rows.reserve(acoToInsert.size());
                for (const auto& aco : acoToInsert) {
                    TFinishedQueryByAcoAndStartTimePartial record{
                        .Key = {.IsTutorial = isTutorial, .AccessControlObject = aco, .MinusStartTime = -TMinusTimestamp(query.StartTime->MicroSeconds()), .QueryId = query.Id},
                        .Engine = query.Engine,
                        .User = query.User,
                        .State = query.State,
                        .FilterFactors = filterFactors,
                    };
                    rows.push_back(record.ToUnversionedRow(rowBuffer, TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetPartialIdMapping()));
                }
                transaction->WriteRows(
                    StateRoot_ + "/" + FinishedQueriesByAcoAndStartTimeTable,
                    TFinishedQueryByAcoAndStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(rows), rowBuffer));
            }
        }
    }

    TListQueriesResult ListQueries(const TListQueriesOptions& options, const std::string& user) override
    {
        bool incomplete = false;
        std::vector<TQuery> queries;
        auto indexSearchOptions = MakeQueryIndexSearchOptions(options, user);
        {
            std::optional<bool> stateFilterDefinesFinishedQuery = options.StateFilter
                ? std::make_optional(IsFinishedState(*options.StateFilter))
                : std::nullopt;
            std::vector<TExtendedCallback<std::vector<TQuery>()>> callbacks;
            if (!options.StateFilter || *stateFilterDefinesFinishedQuery) {
                callbacks.push_back(BIND(&TTimeBasedIndex::SelectFinishedQueries, MakeStrong(this), options, indexSearchOptions, &incomplete));
            }
            if (!options.StateFilter || !*stateFilterDefinesFinishedQuery) {
                callbacks.push_back(BIND(&TTimeBasedIndex::SelectActiveQueries, MakeStrong(this), options, indexSearchOptions));
            }
            queries = RunAsyncAndConcatenate(callbacks);
        }

        if (options.CursorDirection != EOperationSortDirection::None) {
            std::sort(queries.begin(), queries.end(), TCursorComparator(options.CursorDirection));
        }

        if (queries.size() > options.Limit) {
            incomplete = true;
        }
        queries.resize(std::min(queries.size(), options.Limit));

        if (options.SortOrder != EListQueriesSortOrder::Cursor) {
            std::sort(queries.begin(), queries.end(), TSortOrderComparator(options.SortOrder));
        }

        if (!indexSearchOptions.Attributes ||
            std::find(indexSearchOptions.Attributes.Keys().begin(), indexSearchOptions.Attributes.Keys().end(), "access_control_object") != indexSearchOptions.Attributes.Keys().end())
        {
            for (auto& query : queries) {
                ConvertAcoToOldFormat(query);
            }
        }

        return TListQueriesResult {
            .Queries = queries,
            .Incomplete = incomplete,
            .Timestamp = indexSearchOptions.Timestamp,
        };
    }

private:
    void AddFilterConditions(
        TQueryBuilder& builder,
        TYsonString& placeholderValues,
        const TListQueriesOptions& options,
        const std::string& user,
        const std::vector<std::string>& acosForUser,
        const TString& table,
        bool isSuperuser)
    {
        auto fromTime = options.FromTime ? std::make_optional<i64>(options.FromTime->MicroSeconds()) : std::nullopt;
        if (options.CursorDirection == EOperationSortDirection::Future && options.CursorTime) {
            fromTime = i64(options.CursorTime->MicroSeconds()) + 1;
        }

        auto toTime = options.ToTime ? std::make_optional<i64>(options.ToTime->MicroSeconds()) : std::nullopt;
        if (options.CursorDirection == EOperationSortDirection::Past && options.CursorTime) {
            toTime = i64(options.CursorTime->MicroSeconds()) - 1;
        }

        auto placeholdersFluentMap = BuildYsonNodeFluently().BeginMap();
        if (options.UserFilter) {
            builder.AddWhereConjunct("[user] = {UserFilter}");
            placeholdersFluentMap.Item("UserFilter").Value(*options.UserFilter);
        }
        if (options.EngineFilter) {
            builder.AddWhereConjunct("[engine] = {EngineFilter}");
            placeholdersFluentMap.Item("EngineFilter").Value(*options.EngineFilter);
        }
        if (options.StateFilter) {
            builder.AddWhereConjunct("[state] = {StateFilter}");
            placeholdersFluentMap.Item("StateFilter").Value(*options.StateFilter);
        }
        if (options.SubstrFilter) {
            builder.AddWhereConjunct("is_substr({SubstrFilter}, filter_factors)");
            placeholdersFluentMap.Item("SubstrFilter").Value(*options.SubstrFilter);
        }
        builder.AddWhereConjunct("[is_tutorial] = {TutorialFilter}");
        placeholdersFluentMap.Item("TutorialFilter").Value(options.TutorialFilter);

        if (table == "active_queries") {
            if (fromTime) {
                builder.AddWhereConjunct("[start_time] >= " + ToString(fromTime));
            }
            if (toTime) {
                builder.AddWhereConjunct("[start_time] <= " + ToString(toTime));
            }

            builder.AddWhereConjunct("[is_indexed] = TRUE");

            if (!isSuperuser) {
                placeholdersFluentMap.Item("User").Value(user);

                TStringBuilder conditionBuilder;
                TDelimitedStringBuilderWrapper delimitedBuilder(&conditionBuilder, " OR ");

                delimitedBuilder->AppendString("user = {User}");
                for (const auto& aco : acosForUser) {
                    delimitedBuilder->AppendString(Format("list_contains(access_control_objects, \"%v\")", aco));
                }

                builder.AddWhereConjunct(conditionBuilder.Flush());
            }
        } else {
            if (fromTime) {
                builder.AddWhereConjunct("-[minus_start_time] >= " + ToString(fromTime));
            }
            if (toTime) {
                builder.AddWhereConjunct("-[minus_start_time] <= " + ToString(toTime));
            }

            if (!isSuperuser) {
                if (table == FinishedQueriesByUserAndStartTimeTable) {
                    placeholdersFluentMap.Item("User").Value(user);
                    builder.AddWhereConjunct("user = {User}");
                } else if (table == FinishedQueriesByAcoAndStartTimeTable) {
                    placeholdersFluentMap.Item("acosForUser").Value(acosForUser);
                    builder.AddWhereConjunct("access_control_object IN {acosForUser}");
                }
            }
        }

        placeholderValues = ConvertToYsonString(placeholdersFluentMap.EndMap());
    }

    template <typename TRecord>
    void GetQueriesByStartTime(
        const TListQueriesOptions& requestOptions,
        const TTimestamp timestamp,
        const std::string& user,
        const std::vector<std::string>& acosForUser,
        const TString& tableName,
        std::vector<std::pair<TTimestamp, TQueryId>>& results,
        bool isSuperuser = false)
    {
        TQueryBuilder builder;
        TYsonString placeholderValues;
        builder.SetSource(StateRoot_ + "/" + tableName);
        AddFilterConditions(builder, placeholderValues, requestOptions, user, acosForUser, tableName, isSuperuser);
        builder.AddSelectExpression("[minus_start_time]");
        builder.AddSelectExpression("[query_id]");
        // We request for one more in order to understand whether the whole result fits into the limit.
        builder.SetLimit(requestOptions.Limit + 1);
        if (requestOptions.CursorDirection == EOperationSortDirection::Past) {
            builder.AddOrderByAscendingExpression("minus_start_time");
        } else if (requestOptions.CursorDirection == EOperationSortDirection::Future) {
            builder.AddOrderByDescendingExpression("minus_start_time");
        }
        if (tableName == FinishedQueriesByAcoAndStartTimeTable) {
            // We need to combine the same query with different acos into one record
            builder.AddGroupByExpression("minus_start_time");
            builder.AddGroupByExpression("query_id");
        }

        TSelectRowsOptions options;
        options.Timestamp = timestamp;
        options.PlaceholderValues = placeholderValues;
        auto query = builder.Build();
        YT_LOG_DEBUG("Selecting finished queries by start time (Query: %v, Table: %v)", query, tableName);
        auto selectResult = WaitFor(StateClient_->SelectRows(query, options))
            .ValueOrThrow();

        for (const auto& record : ToRecords<TRecord>(selectResult.Rowset)) {
            results.push_back({-record.Key.MinusStartTime, record.Key.QueryId});
        }
    }

    std::vector<TQuery> SelectActiveQueries(const TListQueriesOptions& options, const TQueryIndexSearchOptions& indexSearchOptions)
    {
        try {
            TQueryBuilder builder;
            TYsonString placeholderValues;
            builder.SetSource(StateRoot_ + "/active_queries");
            AddFilterConditions(
                builder,
                placeholderValues,
                options,
                indexSearchOptions.User,
                indexSearchOptions.AcosForUser,
                TString("active_queries"),
                indexSearchOptions.IsSuperuser);
            AddSelectExpressionsFromAttributes(builder, TActiveQueryDescriptor::Get()->GetNameTable(), indexSearchOptions.Attributes);
            AddSelectExpressionsForMerging(builder, indexSearchOptions.Attributes);
            TSelectRowsOptions selectOptions;
            selectOptions.Timestamp = indexSearchOptions.Timestamp;
            selectOptions.PlaceholderValues = placeholderValues;
            auto query = builder.Build();
            YT_LOG_DEBUG("Selecting active queries (Query: %v)", query);
            auto selectResult = WaitFor(StateClient_->SelectRows(query, selectOptions))
                .ValueOrThrow();
            auto records = ToRecords<TActiveQueryPartial>(selectResult.Rowset);
            return PartialRecordsToQueries(records);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting active queries")
                << ex;
        }
    }

    std::vector<TQuery> SelectFinishedQueries(const TListQueriesOptions& options, const TQueryIndexSearchOptions& indexSearchOptions, bool* incomplete)
    {
        if (options.SubstrFilter) {
            YT_LOG_DEBUG(
                "Starting search for the exact occurrence of a substring in finished_queries; this can work slowly (SubstringFilter: %v, UseFullTextSearch: %v)",
                options.SubstrFilter,
                options.UseFullTextSearch);
        }
        try {
            std::vector<TQueryId> admittedQueryIds;
            {
                std::vector<std::pair<TTimestamp, TQueryId>> results;
                if (indexSearchOptions.IsSuperuser && !options.UserFilter) {
                    GetQueriesByStartTime<TFinishedQueryByStartTime>(
                        options,
                        indexSearchOptions.Timestamp,
                        indexSearchOptions.User,
                        indexSearchOptions.AcosForUser,
                        "finished_queries_by_start_time",
                        results,
                        indexSearchOptions.IsSuperuser);
                } else {
                    GetQueriesByStartTime<TFinishedQueryByUserAndStartTime>(
                        options,
                        indexSearchOptions.Timestamp,
                        indexSearchOptions.User,
                        indexSearchOptions.AcosForUser,
                        FinishedQueriesByUserAndStartTimeTable,
                        results,
                        indexSearchOptions.IsSuperuser);
                    if (!indexSearchOptions.AcosForUser.empty()) {
                        GetQueriesByStartTime<TFinishedQueryByAcoAndStartTime>(
                            options,
                            indexSearchOptions.Timestamp,
                            indexSearchOptions.User,
                            indexSearchOptions.AcosForUser,
                            FinishedQueriesByAcoAndStartTimeTable,
                            results,
                            indexSearchOptions.IsSuperuser);
                    }
                    if (options.CursorDirection != EOperationSortDirection::None) {
                        auto compare = [&] (const std::pair<TTimestamp, TQueryId>& lhs, const std::pair<TTimestamp, TQueryId>& rhs) {
                            return options.CursorDirection == EOperationSortDirection::Past ? lhs > rhs : lhs < rhs;
                        };
                        std::sort(results.begin(), results.end(), compare);
                    }
                }
                // Filter duplicates.
                THashSet<TQueryId> usedQueries;
                for (const auto& result : results) {
                    if (usedQueries.contains(result.second)) {
                        continue;
                    }
                    usedQueries.insert(result.second);
                    if (usedQueries.size() > options.Limit) {
                        *incomplete = true;
                        break;
                    }

                    admittedQueryIds.push_back(result.second);
                }
            }
            if (admittedQueryIds.empty()) {
                return {};
            }
            return FetchQueriesByIds(admittedQueryIds, options, indexSearchOptions);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting finished queries")
                << ex;
        }
    }
};

ISearchIndexPtr CreateTimeBasedIndex(IClientPtr stateClient, TYPath stateRoot)
{
    return New<TTimeBasedIndex>(std::move(stateClient), std::move(stateRoot));
}

////////////////////////////////////////////////////////////////////////////////

class TTokenBasedIndex
    : public TSearchIndexBase
{
public:
    TTokenBasedIndex(IClientPtr stateClient, TYPath stateRoot)
        : TSearchIndexBase(std::move(stateClient), std::move(stateRoot))
        , Tokenizer_(CreateRegexTokenizer())
    { }

    void AddQuery(const TQuery& query, ITransactionPtr transaction) override
    {
        ValidateQuery(query);

        auto rowBuffer = New<TRowBuffer>();

        auto accessControlObjects = query.AccessControlObjects ? ConvertTo<std::vector<std::string>>(query.AccessControlObjects) : std::vector<std::string>{};

        auto accessScopes = GetAccessScopes(query.User.value(), accessControlObjects);
        auto tokens = GetTokensFromQuery(Tokenizer_, query.Query.value(), ConvertToNode(query.Annotations)->AsMap(), accessControlObjects, ETokenizationMode::Standard);

        std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator> tokenOccurrences;
        WriteRowsToInvertedIndex(
            accessScopes,
            tokens,
            -static_cast<TMinusTimestamp>(query.StartTime->MicroSeconds()),
            query.Id,
            query.Engine.value(),
            query.User.value(),
            tokenOccurrences,
            rowBuffer,
            transaction);
        UpdateSearchMetaTable(transaction, tokenOccurrences);
    }

    void RemoveQuery(const TQuery& query, ITransactionPtr transaction) override
    {
        ValidateQuery(query);

        auto rowBuffer = New<TRowBuffer>();

        auto accessControlObjects = query.AccessControlObjects ? ConvertTo<std::vector<std::string>>(query.AccessControlObjects) : std::vector<std::string>{};

        auto accessScopes = GetAccessScopes(query.User.value(), accessControlObjects);
        auto tokens = GetTokensFromQuery(Tokenizer_, query.Query.value(), ConvertToNode(query.Annotations)->AsMap(), accessControlObjects, ETokenizationMode::Standard);

        std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator> tokenOccurrences;
        DeleteRowsFromInvertedIndex(
            accessScopes,
            tokens,
            -static_cast<TMinusTimestamp>(query.StartTime->MicroSeconds()),
            query.Id,
            query.Engine.value(),
            query.User.value(),
            tokenOccurrences,
            rowBuffer,
            transaction);
        UpdateSearchMetaTable(transaction, tokenOccurrences);
    }

    void UpdateQuery(const TQuery& query, const TUpdateQueryOptions& options, ITransactionPtr transaction) override
    {
        ValidateQuery(query);

        auto tokenOccurrences = UpdateSearchIndexTable(
            query,
            Tokenizer_,
            options.NewAccessControlObjects,
            options.NewAnnotations,
            query.User.value(),
            transaction);
        UpdateSearchMetaTable(transaction, tokenOccurrences);
    }

    TListQueriesResult ListQueries(const TListQueriesOptions& options, const std::string& user) override
    {
        static_assert(TSearchIndexRecordDescriptor::FieldCount == 7);
        static_assert(TSearchMetaRecordDescriptor::FieldCount == 6);

        auto indexSearchOptions = MakeQueryIndexSearchOptions(options, user);

        try {
            bool incomplete = false;
            auto [tokens, accessScopes] = GetTokensAndAccessScopesForSearch(options, indexSearchOptions);
            YT_LOG_DEBUG(
                "Search request parsed (Tokens: %v, AccessScopes: %v)",
                MakeShrunkFormattableView(tokens, TDefaultFormatter(), ShrunkFormattableViewLimit),
                MakeShrunkFormattableView(accessScopes, TDefaultFormatter(), ShrunkFormattableViewLimit)
            );

            auto rarestKeys = GetRarestKeys(options, indexSearchOptions, tokens, accessScopes);
            auto selectedIndexRecords = SelectAndSortRecordsFromInvertedIndex(rarestKeys, options, indexSearchOptions);

            if (selectedIndexRecords.size() > static_cast<ui64>(options.Limit)) {
                selectedIndexRecords.resize(options.Limit);
                incomplete = true;
            }
            std::vector<TQueryId> selectedQueryIds;
            selectedQueryIds.reserve(selectedIndexRecords.size());
            for (const auto& queryInfo : selectedIndexRecords) {
                selectedQueryIds.push_back(queryInfo.first);
            }

            auto queries = FetchQueriesByIds(selectedQueryIds, options, indexSearchOptions);

            return TListQueriesResult{
                .Queries = queries,
                .Incomplete = incomplete,
                .Timestamp = indexSearchOptions.Timestamp,
            };
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error while selecting queries using token-based index")
                << ex;
        }
    }

private:
    const IQueryTokenizerPtr Tokenizer_;

    std::vector<TParsedToken> GetTokensFromQuery(
        IQueryTokenizerPtr tokenizer,
        const std::string& query,
        IMapNodePtr annotations,
        const std::vector<std::string>& accessControlObjects,
        ETokenizationMode tokenizationMode)
    {
        auto tokens = Tokenize(query, tokenizer, tokenizationMode);

        std::vector<TParsedToken> metaTokens;
        if (annotations) {
            TStringBuilder builder;
            for (const auto& [key, childNode] : annotations->GetChildren()) {
                if (!UnindexedAnnotationKeys.contains(key)) {
                    builder.AppendFormat(
                        "%v %v ",
                        ConvertToYsonString(childNode, EYsonFormat::Text).ToString(),
                        key
                    );
                }
            }
            metaTokens = Tokenize(builder.Flush(), tokenizer, tokenizationMode);
        }

        std::vector<TParsedToken> acoTokens;
        for (const auto& aco : accessControlObjects) {
            if (aco != "nobody") {
                acoTokens.emplace_back(AcoPrefix + aco, 1);
            }
        }

        std::unordered_map<std::string, i64> tokenMap;
        for (const auto& token : tokens) {
            tokenMap[token.Token] += token.Occurrences;
        }
        for (const auto& token : metaTokens) {
            tokenMap[token.Token] += token.Occurrences;
        }
        for (const auto& token : acoTokens) {
            tokenMap[token.Token] += token.Occurrences;
        }
        std::vector<TParsedToken> result;
        result.reserve(tokenMap.size());
        for (const auto& [key, occurrences] : tokenMap) {
            result.emplace_back(key, occurrences);
        }

        return result;
    }

    TSearchMetaRecordKey MakeSearchMetaKeyFromSearchIndexKey(const TSearchIndexRecordKey& source)
    {
        static_assert(TSearchIndexRecordDescriptor::FieldCount == 7);
        static_assert(TSearchMetaRecordDescriptor::FieldCount == 6);
        return TSearchMetaRecordKey{
            .AccessScope = source.AccessScope,
            .Token = source.Token,
            .Engine = source.Engine,
            .User = source.User,
        };
    }

    void UpdateSearchMetaTable(
        ITransactionPtr transaction,
        const std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator>& tokenOccurrences)
    {
        static_assert(TSearchMetaRecordDescriptor::FieldCount == 6);

        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;

        for (const auto& [key, occurrences] : tokenOccurrences) {
            TSearchMetaRecord newRecord {
                .Key = key,
                .TotalOccurrences = occurrences.first,
                .UniqueQueries = occurrences.second,
            };
            rows.push_back(newRecord.ToUnversionedRow(rowBuffer, TSearchMetaRecordDescriptor::Get()->GetPartialIdMapping(), EValueFlags::Aggregate));
        }

        transaction->WriteRows(
            StateRoot_ + "/" + SearchMetaTable,
            TSearchMetaRecordDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(rows), rowBuffer),
            {},
            ELockType::SharedWrite);
    }

    std::vector<std::pair<std::string, std::string>> GenerateEngineUserPairs(const std::string& engine, const std::string& user, const std::string& accessScope)
    {
        std::vector<std::pair<std::string, std::string>> result{
            {NoEngineFilterString, NoUserFilterString},
            {engine, NoUserFilterString},
        };
        if (accessScope.starts_with(AcoPrefix)) {
            result.emplace_back(NoEngineFilterString, user);
            result.emplace_back(engine, user);
        }
        return result;
    }

    void WriteRowsToInvertedIndex(
        const std::vector<std::string>& accessScopes,
        const std::vector<TParsedToken>& tokens,
        TMinusTimestamp minusStartTime,
        TQueryId queryId,
        EQueryEngine engine,
        const std::string& user,
        std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator>& tokenOccurrences,
        TRowBufferPtr rowBuffer,
        ITransactionPtr transaction)
    {
        static_assert(TSearchIndexRecordDescriptor::FieldCount == 7);
        if (accessScopes.empty() || tokens.empty()) {
            return;
        }

        std::vector<TUnversionedRow> rowsToInsert;

        auto updateTokenOccurrences = [&tokenOccurrences](const TSearchMetaRecordKey& metaKey, const TParsedToken& parsedToken) {
            auto& tokenOccurrence = tokenOccurrences[metaKey];
            // First field counts the total number of tokens occurrences.
            tokenOccurrence.first += parsedToken.Occurrences;
            // And the second field contains the number of unique queries with specified token.
            ++tokenOccurrence.second;
        };

        for (const auto& accessScope : accessScopes) {
            for (const auto& parsedToken : tokens) {
                TSearchIndexRecord newRecord{
                    .Key = {
                        .AccessScope = accessScope,
                        .Token = parsedToken.Token,
                        .Engine = NoEngineFilterString,
                        .User = NoUserFilterString,
                        .MinusStartTime = minusStartTime,
                        .QueryId = queryId,
                    },
                    .OccurrenceCount = parsedToken.Occurrences,
                };
                for (const auto& [engine, user] : GenerateEngineUserPairs(Format("%lv", engine), user, accessScope)) {
                    newRecord.Key.Engine = engine;
                    newRecord.Key.User = user;
                    rowsToInsert.push_back(newRecord.ToUnversionedRow(rowBuffer, TSearchIndexRecordDescriptor::Get()->GetPartialIdMapping()));
                    updateTokenOccurrences(MakeSearchMetaKeyFromSearchIndexKey(newRecord.Key), parsedToken);
                }
            }
        }

        transaction->WriteRows(
            StateRoot_ + "/" + SearchIndexTable,
            TSearchIndexRecordDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(rowsToInsert), rowBuffer));
    }

    void DeleteRowsFromInvertedIndex(
        const std::vector<std::string>& accessScopes,
        const std::vector<TParsedToken>& tokens,
        TMinusTimestamp minusStartTime,
        TQueryId queryId,
        EQueryEngine engine,
        const std::string& user,
        std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator>& tokenOccurrences,
        TRowBufferPtr rowBuffer,
        ITransactionPtr transaction)
    {
        static_assert(TSearchIndexRecordDescriptor::FieldCount == 7);
        static_assert(TSearchMetaRecordDescriptor::FieldCount == 6);

        if (accessScopes.empty() || tokens.empty()) {
            return;
        }

        std::vector<TUnversionedRow> keysToDelete;
        keysToDelete.reserve(accessScopes.size() * tokens.size());

        auto updateTokenOccurrences = [&tokenOccurrences](const TSearchMetaRecordKey& metaKey, const TParsedToken& parsedToken) {
            auto& tokenOccurrence = tokenOccurrences[metaKey];
            // First field counts the total number of tokens occurrences.
            tokenOccurrence.first -= parsedToken.Occurrences;
            // And the second field contains the number of unique queries with specified token.
            --tokenOccurrence.second;
        };

        for (const auto& accessScope : accessScopes) {
            for (const auto& parsedToken : tokens) {
                TSearchIndexRecordKey keyToDelete{
                    .AccessScope = accessScope,
                    .Token = parsedToken.Token,
                    .Engine = NoEngineFilterString,
                    .User = NoUserFilterString,
                    .MinusStartTime = minusStartTime,
                    .QueryId = queryId,
                };
                for (const auto& [engine, user] : GenerateEngineUserPairs(Format("%lv", engine), user, accessScope)) {
                    keyToDelete.Engine = engine;
                    keyToDelete.User = user;
                    keysToDelete.push_back(keyToDelete.ToKey(rowBuffer));
                    updateTokenOccurrences(MakeSearchMetaKeyFromSearchIndexKey(keyToDelete), parsedToken);
                }
            }
        }

        transaction->DeleteRows(
            StateRoot_ + "/" + SearchIndexTable,
            TSearchIndexRecordDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keysToDelete), rowBuffer));
    }

    std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator> UpdateSearchIndexTable(
        const TQuery& query,
        IQueryTokenizerPtr tokenizer,
        const std::optional<std::vector<std::string>>& newAccessControlObjects,
        IMapNodePtr newAnnotations,
        const std::string& user,
        ITransactionPtr transaction)
    {
        auto rowBuffer = New<TRowBuffer>();

        if (!query.Engine) {
            THROW_ERROR_EXCEPTION("Query engine is lost in query %v", query.Id);
        }

        auto previousAccessControlObjects =
            query.AccessControlObjects && *query.AccessControlObjects ? ConvertTo<std::vector<std::string>>(*query.AccessControlObjects) : std::vector<std::string>{};
        auto [acosToDelete, acosToInsert] = GetAccessControlDiff(previousAccessControlObjects, newAccessControlObjects);

        if (!query.Query) {
            THROW_ERROR_EXCEPTION("Query text is lost in query %v", query.Id);
        }
        auto previousQueryAnnotationsMap = ConvertToNode(query.Annotations)->AsMap();
        auto tokens = GetTokensFromQuery(tokenizer, query.Query.value(), previousQueryAnnotationsMap, previousAccessControlObjects, ETokenizationMode::Standard);

        acosToDelete.erase(std::remove(acosToDelete.begin(), acosToDelete.end(), "nobody"), acosToDelete.end());
        acosToInsert.erase(std::remove(acosToInsert.begin(), acosToInsert.end(), "nobody"), acosToInsert.end());

        auto acoToAccessScope = [](const std::string& aco) {
            return AcoPrefix + aco;
        };

        std::transform(acosToDelete.begin(), acosToDelete.end(), acosToDelete.begin(), acoToAccessScope);
        std::transform(acosToInsert.begin(), acosToInsert.end(), acosToInsert.begin(), acoToAccessScope);

        auto minusStartTime = -static_cast<TMinusTimestamp>(query.StartTime->MicroSeconds());

        std::map<TSearchMetaRecordKey, std::pair<i64, i64>, TSearchMetaRecordKeyComparator> tokenOccurrences;

        DeleteRowsFromInvertedIndex(
            acosToDelete,
            tokens,
            minusStartTime,
            query.Id,
            query.Engine.value(),
            user,
            tokenOccurrences,
            rowBuffer,
            transaction);
        WriteRowsToInvertedIndex(
            acosToInsert,
            tokens,
            minusStartTime,
            query.Id,
            query.Engine.value(),
            user,
            tokenOccurrences,
            rowBuffer,
            transaction);

        std::vector<TParsedToken> tokensToDelete;
        std::vector<TParsedToken> tokensToInsert;

        auto newTokens = GetTokensFromQuery(
            tokenizer,
            query.Query.value(),
            newAnnotations ? newAnnotations : previousQueryAnnotationsMap,
            newAccessControlObjects ? newAccessControlObjects.value() : previousAccessControlObjects,
            ETokenizationMode::Standard);

        std::unordered_map<std::string, i64> previousMap;
        std::unordered_map<std::string, i64> newMap;

        for (const auto& token : tokens) {
            previousMap[token.Token] += token.Occurrences;
        }
        for (const auto& token : newTokens) {
            newMap[token.Token] += token.Occurrences;
        }

        for (const auto& token : tokens) {
            if (!newMap.contains(token.Token) || newMap[token.Token] != token.Occurrences) {
                tokensToDelete.emplace_back(token.Token, previousMap[token.Token]);
            }
        }
        for (const auto& token : newTokens) {
            if (!previousMap.contains(token.Token) || previousMap[token.Token] != token.Occurrences) {
                tokensToInsert.emplace_back(token.Token, newMap[token.Token]);
            }
        }

        auto accessScopes = GetAccessScopes(user, newAccessControlObjects ? newAccessControlObjects.value() : previousAccessControlObjects);

        DeleteRowsFromInvertedIndex(
            accessScopes,
            tokensToDelete,
            minusStartTime,
            query.Id,
            query.Engine.value(),
            user,
            tokenOccurrences,
            rowBuffer,
            transaction);
        WriteRowsToInvertedIndex(
            accessScopes,
            tokensToInsert,
            minusStartTime,
            query.Id,
            query.Engine.value(),
            user,
            tokenOccurrences,
            rowBuffer,
            transaction);

        return tokenOccurrences;
    }

    std::tuple<std::vector<std::string>, std::vector<std::string>> GetTokensAndAccessScopesForSearch(
        const TListQueriesOptions& options,
        const TQueryIndexSearchOptions& indexSearchOptions)
    {
        std::vector<std::string> tokens;
        std::vector<std::string> accessScopes;
        {
            if (indexSearchOptions.IsSuperuser) {
                if (options.UserFilter) {
                    accessScopes.push_back(UserPrefix + options.UserFilter.value());
                } else {
                    accessScopes.push_back(SuperuserSearchAccessScope);
                }
            } else {
                if (!options.UserFilter || options.UserFilter.value() != indexSearchOptions.User) {
                    for (const auto& aco : indexSearchOptions.AcosForUser) {
                        accessScopes.push_back(AcoPrefix + aco);
                    }
                }
                if (!options.UserFilter || options.UserFilter.value() == indexSearchOptions.User) {
                    accessScopes.push_back(UserPrefix + indexSearchOptions.User);
                }
            }

            YT_VERIFY(options.SubstrFilter.has_value());
            auto searchQueryText = options.SubstrFilter.value();

            for (const auto& parsedToken : Tokenize(searchQueryText, Tokenizer_, ETokenizationMode::ForSearch)) {
                tokens.push_back(parsedToken.Token);
            }
        }
        return {tokens, accessScopes};
    }

    std::vector<TSearchMetaRecordKey> GetRarestKeys(
        const TListQueriesOptions& options,
        const TQueryIndexSearchOptions& indexSearchOptions,
        const std::vector<std::string>& tokens,
        const std::vector<std::string>& accessScopes)
    {
        auto placeholderValuesMap = BuildYsonStringFluently().BeginMap();

        TQueryBuilder builder;
        builder.SetSource(StateRoot_ + "/" + SearchMetaTable);

        builder.AddSelectExpression("[access_scope]");
        builder.AddSelectExpression("[token]");

        builder.AddWhereConjunct("[access_scope] in {AccessScopes}");
        placeholderValuesMap.Item("AccessScopes").Value(accessScopes);

        if (options.SearchByTokenPrefix) {
            TStringBuilder conditionBuilder;
            TDelimitedStringBuilderWrapper delimitedBuilder(&conditionBuilder, " OR ");
            int sequenceNumber = 0;
            for (const auto& token : tokens) {
                delimitedBuilder->AppendFormat("is_prefix({Token%v}, [token])", sequenceNumber);
                placeholderValuesMap.Item("Token" + std::to_string(sequenceNumber)).Value(token);
                ++sequenceNumber;
            }
            if (sequenceNumber > 0) {
                builder.AddWhereConjunct(conditionBuilder.Flush());
            }
        } else {
            builder.AddWhereConjunct("[token] in {Tokens}");
            placeholderValuesMap.Item("Tokens").Value(tokens);
        }

        builder.AddWhereConjunct("[engine] = {EngineFilter}");
        placeholderValuesMap.Item("EngineFilter").Value(options.EngineFilter
            ? Format("%lv", options.EngineFilter.value())
            : NoEngineFilterString);

        builder.AddWhereConjunct("[user] = {User}");
        placeholderValuesMap.Item("User").Value(options.UserFilter && options.UserFilter.value() != indexSearchOptions.User && !indexSearchOptions.IsSuperuser
            ? options.UserFilter.value()
            : NoUserFilterString);

        builder.AddWhereConjunct("[total_occurrences] > 0");
        builder.AddWhereConjunct("[unique_queries] > 0");

        builder.AddOrderByAscendingExpression("[unique_queries], [total_occurrences]");
        builder.SetLimit(TopRarestTokenLimit);
        auto query = builder.Build();

        TSelectRowsOptions selectOptions;
        selectOptions.Timestamp = indexSearchOptions.Timestamp;
        selectOptions.PlaceholderValues = placeholderValuesMap.EndMap();

        YT_LOG_DEBUG("Selecting top rarest tokens from meta table (Query: %v, User: %v)", query, indexSearchOptions.User);

        auto selectResult = WaitFor(StateClient_->SelectRows(query, selectOptions))
            .ValueOrThrow();
        auto records = ToRecords<TSearchMetaRecordPartial>(selectResult.Rowset);

        std::vector<TSearchMetaRecordKey> topRarestKeys;
        topRarestKeys.reserve(records.size());
        for (const auto& record : records) {
            topRarestKeys.push_back(record.Key);
        }
        return topRarestKeys;
    }

    std::vector<TSearchIndexRecordPartial> SelectRecordFromInvertedIndex(
        TSearchMetaRecordKey key,
        const TListQueriesOptions& options,
        const TQueryIndexSearchOptions& indexSearchOptions)
    {
        auto fromTime = options.FromTime ? std::make_optional<i64>(options.FromTime->MicroSeconds()) : std::nullopt;
        if (options.CursorDirection == EOperationSortDirection::Future && options.CursorTime) {
            fromTime = i64(options.CursorTime->MicroSeconds()) + 1;
        }

        auto toTime = options.ToTime ? std::make_optional<i64>(options.ToTime->MicroSeconds()) : std::nullopt;
        if (options.CursorDirection == EOperationSortDirection::Past && options.CursorTime) {
            toTime = i64(options.CursorTime->MicroSeconds()) - 1;
        }

        auto placeholderValuesMap = BuildYsonStringFluently().BeginMap();

        TQueryBuilder builder;
        builder.SetSource(StateRoot_ + "/" + SearchIndexTable);

        builder.AddSelectExpression("[query_id]");
        builder.AddSelectExpression("[minus_start_time]");

        builder.AddWhereConjunct("[access_scope] = {AccessScope}");
        placeholderValuesMap.Item("AccessScope").Value(key.AccessScope);

        builder.AddWhereConjunct("[token] = {Token}");
        placeholderValuesMap.Item("Token").Value(key.Token);

        builder.AddWhereConjunct("[engine] = {EngineFilter}");
        placeholderValuesMap.Item("EngineFilter").Value(options.EngineFilter
            ? Format("%lv", options.EngineFilter.value())
            : NoEngineFilterString);

        builder.AddWhereConjunct("[user] = {User}");
        placeholderValuesMap.Item("User").Value(options.UserFilter && options.UserFilter.value() != indexSearchOptions.User && !indexSearchOptions.IsSuperuser
            ? options.UserFilter.value()
            : NoUserFilterString);

        if (options.CursorDirection == EOperationSortDirection::Future) {
            builder.AddOrderByAscendingExpression("[access_scope], [token], [engine], [user]");
            builder.AddOrderByDescendingExpression("[minus_start_time]");
        }

        if (fromTime) {
            builder.AddWhereConjunct("-[minus_start_time] >= " + ToString(fromTime));
        }
        if (toTime) {
            builder.AddWhereConjunct("-[minus_start_time] <= " + ToString(toTime));
        }

        builder.SetLimit(options.Limit + 1);

        auto query = builder.Build();
        YT_LOG_DEBUG("Selecting queries with rare token and access_scope (Query: %v, Limit: %v, Token: %v, AccessScope: %v)", options.Limit, query, key.Token, key.AccessScope);

        TSelectRowsOptions selectOptions;
        selectOptions.Timestamp = indexSearchOptions.Timestamp;
        selectOptions.PlaceholderValues = placeholderValuesMap.EndMap();

        auto selectResult = WaitFor(StateClient_->SelectRows(query, selectOptions))
            .ValueOrThrow();
        auto records = ToRecords<TSearchIndexRecordPartial>(selectResult.Rowset);
        return records;
    };

    std::vector<std::pair<TQueryId, TMinusTimestamp>> SelectAndSortRecordsFromInvertedIndex(
        const std::vector<TSearchMetaRecordKey>& rarestKeys,
        const TListQueriesOptions& options,
        const TQueryIndexSearchOptions& indexSearchOptions)
    {
        std::vector<TExtendedCallback<std::vector<TSearchIndexRecordPartial>()>> callbacks;
        for (const auto& key : rarestKeys) {
            callbacks.push_back(BIND(&TTokenBasedIndex::SelectRecordFromInvertedIndex, MakeStrong(this), key, options, indexSearchOptions));
        }
        auto selectedQueries = RunAsyncAndConcatenate(callbacks);

        // filter duplicates
        THashMap<TQueryId, TMinusTimestamp> foundIdsMap;
        for (const auto& query : selectedQueries) {
            foundIdsMap[query.Key.QueryId] = query.Key.MinusStartTime;
        }

        auto selectedQueryIds = std::vector<std::pair<TQueryId, TMinusTimestamp>>(foundIdsMap.begin(), foundIdsMap.end());

        auto compare = [&] (const std::pair<TQueryId, TMinusTimestamp>& lhs, const std::pair<TQueryId, TMinusTimestamp>& rhs) {
            return options.CursorDirection == EOperationSortDirection::Past
                ? std::tie(lhs.second, lhs.first) < std::tie(rhs.second, rhs.first)
                : std::tie(lhs.second, lhs.first) > std::tie(rhs.second, rhs.first);
        };
        std::sort(selectedQueryIds.begin(), selectedQueryIds.end(), compare);

        return selectedQueryIds;
    }
};

ISearchIndexPtr CreateTokenBasedIndex(IClientPtr stateClient, TYPath stateRoot)
{
    return New<TTokenBasedIndex>(std::move(stateClient), std::move(stateRoot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
