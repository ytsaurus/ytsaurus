#include "select_query_executor.h"

#include "attribute_values_consumer.h"
#include "config.h"
#include "fetchers.h"
#include "helpers.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "query_executor_helpers.h"
#include "select_continuation.h"
#include "type_handler.h"
#include "db_config.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/library/query/filter_matcher.h>

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/query/base/helpers.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NTracing;

using namespace NAccessControl;

using NYT::NQueryClient::TSourceLocation;
using NYT::NQueryClient::NAst::TQuery;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TSelectQueryOptions& options,
    TStringBuf /*format*/)
{
    TDelimitedStringBuilderWrapper wrapper(builder);

    builder->AppendChar('{');

    wrapper->AppendFormat("FetchValues: %v", options.FetchValues);
    wrapper->AppendFormat("FetchTimestamps: %v", options.FetchTimestamps);
    wrapper->AppendFormat("Offset: %v", options.Offset);
    wrapper->AppendFormat("Limit: %v", options.Limit);
    wrapper->AppendFormat("MasterSideLimit: %v", options.MasterSideLimit);
    wrapper->AppendFormat("ContinuationToken: %v", options.ContinuationToken);
    wrapper->AppendFormat("FetchRootObject: %v", options.FetchRootObject);
    wrapper->AppendFormat("CheckReadPermissions: %v", options.CheckReadPermissions);

    builder->AppendChar('}');
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TDBFieldRef> GetContinuationTokenRequiredFields(
    IObjectTypeHandler* typeHandler,
    const TScalarAttributeIndexDescriptor* indexedDescriptor,
    bool indexPrimaryKeyInContinuationToken)
{
    std::vector<TDBFieldRef> result;
    if (indexedDescriptor && indexPrimaryKeyInContinuationToken) {
        const auto& tableName = indexedDescriptor->Table->GetName();
        // Several rows in the repeated index represent a single object.
        // If you sort by index_key, then rows of one object can be split
        // and read by several queries within a continuation resulting in
        // duplicates. Therefore, it is necessary to use object_key
        // in the continuation token to exclude duplicates.
        const auto& fields = indexedDescriptor->Repeated
            ? indexedDescriptor->Table->ObjectTableKey
            : indexedDescriptor->Table->GetKeyFields(/*filterEvaluatedFields*/ false);
        result.reserve(fields.size());
        for (const auto* field : fields) {
            result.push_back({.TableName = tableName, .Name = field->Name, .Evaluated = field->Evaluated});
        }
    } else {
        const auto& tableName = typeHandler->GetTable()->GetName();
        const auto& fields = typeHandler->GetTable()->GetKeyFields(/*filterEvaluatedFields*/ false);
        result.reserve(fields.size());
        for (const auto* field : fields) {
            result.push_back({.TableName = tableName, .Name = field->Name, .Evaluated = field->Evaluated});
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TSelectQueryExecutor
    : public ISelectQueryExecutor
    , public TQueryExecutorBase
{
public:
    TSelectQueryExecutor(
        TTransactionPtr transaction,
        TObjectTypeValue type,
        const std::optional<TObjectFilter>& filter,
        const TAttributeSelector& selector,
        const TObjectOrderBy& orderBy,
        const TSelectQueryOptions& options,
        const std::optional<TIndex>& index)
        : TQueryExecutorBase(std::move(transaction))
        , Type_(type)
        , Filter_(filter)
        , Selector_(selector)
        , OrderBy_(orderBy)
        , Index_(index)
        , Options_(options)
    { }

    TSelectQueryResultWithPayload Execute() && override
    {
        auto consumerGroup = MakeDefaultAttributeValuesConsumerGroup();
        auto result = std::move(*this).ExecuteConsuming(consumerGroup.get());
        TSelectQueryResultWithPayload resultWithPayload;
        resultWithPayload.Objects = consumerGroup->Finalize();
        resultWithPayload.ContinuationToken = std::move(result.ContinuationToken);
        resultWithPayload.LimitSatisfied = result.LimitSatisfied;
        return resultWithPayload;
    }

    TSelectQueryResult ExecuteConsuming(IAttributeValuesConsumerGroup* consumerGroup) const override
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSelectQueryExecutor::ExecuteConsuming"));

        auto limit = Options_.Limit;
        if (limit && *limit < 0) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments, "Negative limit value");
        }

        auto masterSideLimit = Options_.MasterSideLimit;
        if (masterSideLimit && *masterSideLimit <= 0) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Zero or negative master side limit value");
        }

        auto offset = Options_.Offset;
        if (offset && *offset < 0) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments, "Negative offset value");
        }

        const auto& continuationToken = Options_.ContinuationToken;

        if (continuationToken) {
            ValidateSelectContinuationTokenVersion(*continuationToken, Bootstrap_);
        }

        if (offset && continuationToken) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Offset and continuation token cannot be both specified");
        }

        const bool needOrderedOutput = offset || limit || continuationToken || !OrderBy_.Expressions.empty();

        // ORDER BY requires LIMIT to be specified.
        bool syntheticLimit = false;
        if (needOrderedOutput && !limit) {
            syntheticLimit = true;
            limit = Transaction_->GetConfig()->GetOutputRowLimit(/*orderBy*/ !OrderBy_.Expressions.empty());
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(Type_);

        auto queryContext = MakeQueryContext(
            Bootstrap_,
            Type_,
            Transaction_->GetSession(),
            Index_ ? typeHandler->GetIndexDescriptorOrThrow(Index_->Name) : nullptr,
            /*allowAnnotations*/ true);

        std::vector<TSelectContinuationTokenEntry> continuationTokenEntries;
        continuationTokenEntries.reserve(OrderBy_.Expressions.size());
        for (const auto& orderByExpression : OrderBy_.Expressions) {
            continuationTokenEntries.push_back(TSelectContinuationTokenEntry(orderByExpression));
        }
        if (needOrderedOutput) {
            const bool useIndexPrimaryKey = Transaction_->GetConfig()->AllowIndexPrimaryKeyInContinuationToken;
            auto fields = GetContinuationTokenRequiredFields(
                typeHandler,
                queryContext->GetIndexDescriptor(),
                useIndexPrimaryKey);
            for (const auto& field : fields) {
                TSelectContinuationTokenEntry entry;
                // Evaluated fields are not represented as attributes,
                // so use raw DB expression instead of object expression.
                entry.DBExpression = field.Name;
                entry.DBTableName = field.TableName;
                entry.DBEvaluated = field.Evaluated;
                continuationTokenEntries.push_back(std::move(entry));
            }
        }

        TFetcherContext fetcherContext(queryContext.get(), Transaction_.Get());
        fetcherContext.RegisterKeyAndParentKeyFields(typeHandler);

        TAttributePermissionsCollector permissions;

        std::vector<TResolveAttributeResult> resolvedAttributes;
        resolvedAttributes.reserve(Selector_.Paths.size());
        for (const auto& path : Selector_.Paths) {
            auto resolveResult = ResolveAttribute(typeHandler, path, permissions);
            resolvedAttributes.push_back(resolveResult);
        }

        auto timestampFetchers = BuildTimestampFetchers(
            &fetcherContext,
            Options_.FetchTimestamps,
            resolvedAttributes,
            Transaction_->GetConfig()->VersionedSelectEnabled);

        auto selectorFetchers = BuildAttributeFetchers(
            &fetcherContext,
            resolvedAttributes,
            Options_.FetchRootObject,
            Transaction_->GetConfig()->FetchRootOptimizationLevel);

        THashSet<NYPath::TYPath> pathsToCheckReadAccess =
            {Selector_.Paths.begin(), Selector_.Paths.end()};
        if (pathsToCheckReadAccess.empty()) {
            pathsToCheckReadAccess.insert("");
        }

        std::vector<IFetcherPtr> continuationTokenFetchers;
        continuationTokenFetchers.reserve(continuationTokenEntries.size());
        TExpressionList continuationTokenEntryExpressions;
        continuationTokenEntryExpressions.reserve(continuationTokenEntries.size());
        for (const auto& entry : continuationTokenEntries) {
            entry.ValidateExactlyOneExpression();

            if (entry.ObjectExpression) {
                continuationTokenFetchers.push_back(MakeObjectExpressionFetcher(
                    entry.ObjectExpression,
                    /*permissionCollector*/ permissions,
                    /*usedPathsCollector*/ [&pathsToCheckReadAccess] (const TAttributeSchema* schema) {
                        pathsToCheckReadAccess.insert(schema->GetPath());
                    },
                    typeHandler,
                    &fetcherContext));
            } else {
                YT_VERIFY(entry.DBExpression);
                // Only field DB expressions are supported for now.
                continuationTokenFetchers.push_back(MakeDbFieldFetcher(
                    &fetcherContext,
                    TDBFieldRef{
                        .TableName = entry.DBTableName,
                        .Name = entry.DBExpression,
                        .Evaluated = entry.DBEvaluated,
                    }));
            }

            continuationTokenEntryExpressions.push_back(
                TryMakeScalarExpression(
                    continuationTokenFetchers.back(),
                    /*wrapEvaluated*/ Transaction_->GetConfig()->IgnoreContinuationSelectivity,
                    /*where*/ "continuation token"));
        }

        // The index contains only live objects, so filter by `removal_time` is not needed (config
        // option provided just in case).
        auto filterExpression = Index_
            ? queryContext->New<TLiteralExpression>(TSourceLocation(), TLiteralValue(true))
            : BuildObjectFilterByNullColumn(queryContext.get(), &ObjectsTable.Fields.MetaRemovalTime);

        if (Bootstrap_->GetDBConfig().EnableFinalizers &&
            !Options_.FetchFinalizingObjects.value_or(Transaction_->GetConfig()->FetchFinalizingObjectsByDefault))
        {
            filterExpression = NQueryClient::BuildAndExpression(
                queryContext.get(),
                filterExpression,
                BuildObjectFilterByNullColumn(queryContext.get(), &FinalizationStartTimeField));
        }

        TFilterResolveResult computedFilterResult;
        TFilterResolveResult nonComputedFilterResult;
        std::vector<IFetcherPtr> computedFilterFetchers;

        if (Filter_) {
            auto [nonComputedFilterExpr, computedFilterExpr] = BuildFilterExpressions(
                queryContext.get(),
                *Filter_,
                Transaction_->GetConfig()->EnableComputedFilter);

            if (computedFilterExpr) {
                if (!nonComputedFilterExpr && !Transaction_->FullScanAllowed()) {
                    THROW_ERROR_EXCEPTION("Request contains only computed filter, but fullscan is not allowed");
                }
                computedFilterResult = ResolveFilterExpression(computedFilterExpr, queryContext.get(), /*isComputed*/ true);
                computedFilterFetchers = BuildAttributeFetchers(
                    &fetcherContext,
                    computedFilterResult.ResolvedAttributes,
                    Options_.FetchRootObject,
                    Transaction_->GetConfig()->FetchRootOptimizationLevel);

                for (const auto& path : computedFilterResult.FullPaths) {
                    pathsToCheckReadAccess.insert(path);
                }
            }

            if (nonComputedFilterExpr) {
                auto nonComputedResult = ResolveFilterExpression(nonComputedFilterExpr, queryContext.get());
                for (const auto& path : nonComputedFilterResult.FullPaths) {
                    pathsToCheckReadAccess.insert(path);
                }
                filterExpression = NQueryClient::BuildAndExpression(
                    queryContext.get(),
                    nonComputedResult.FilterExpression,
                    filterExpression);
            }
        }

        queryContext->GetQuery()->SelectExprs = fetcherContext.GetSelectExpressions();
        EnsureNonEmptySelectExpressions(queryContext.get(), queryContext->GetQuery());

        bool checkReadPermissions = Options_.CheckReadPermissions;
        bool limitedRead = false;

        if (checkReadPermissions) {
            checkReadPermissions = false;
            limitedRead = true;

            THashSet<NYPath::TYPath> readablePaths;
            for (const auto& path : pathsToCheckReadAccess) {
                if (IsSchemaPermissionAllowedFor(path, TAccessControlPermissionValues::Read)) {
                    readablePaths.insert(path);
                    continue;
                }

                checkReadPermissions = true;
                if (!IsSchemaPermissionAllowedFor(path, TAccessControlPermissionValues::LimitedRead)) {
                    // Limited read only applies if all unreadable paths are limited-readable.
                    limitedRead = false;
                }
            }
            for (const auto& path : readablePaths) {
                pathsToCheckReadAccess.erase(path);
            }

            // No limited read needed if everything is readable.
            limitedRead = limitedRead && checkReadPermissions;
        }

        if (checkReadPermissions) {
            Transaction_->EnableAccessControlPreload();
        }

        if (continuationToken) {
            ValidateSelectContinuationTokenReceivedEntries(
                continuationToken->EvaluatedEntries,
                continuationTokenEntries);

            auto continuationTokenFilterExpression = CreateContinuationTokenFilterExpression(
                continuationToken->EvaluatedEntries,
                continuationTokenEntryExpressions,
                /*holder*/ queryContext.get());

            if (continuationTokenFilterExpression) {
                filterExpression = NQueryClient::BuildAndExpression(
                    queryContext.get(),
                    filterExpression,
                    continuationTokenFilterExpression);
            }
        }

        // Events must be always ordered by continuation token because otherwise combined result of
        // several consecutive continuations may contain duplicates or gaps.
        //
        // ORDER BY table key is redundant when LIMIT is specified, but it is zero-cost in this case
        // and can be used as a sanity check.
        for (int index = 0; index < std::ssize(continuationTokenEntries); ++index) {
            const auto& entry = continuationTokenEntries[index];
            auto expression = TryMakeScalarExpression(
                continuationTokenFetchers[index],
                /*wrapEvaluated*/ false,
                /*where*/ "order by expression");
            queryContext->GetQuery()->OrderExpressions.push_back({
                {expression},
                entry.Descending});
        }

        bool needPermissionsValidation = !permissions.GetReadPermissions().empty();
        if (needPermissionsValidation &&
            typeHandler->GetAccessControlParentType() != TObjectTypeValues::Null)
        {
            // Disallow permission checks involving parent objects, except for the superuser.
            Bootstrap_->GetAccessControlManager()->ValidateSuperuser(Format(
                "select attributes with special read permissions of objects of type %v",
                Type_));
            needPermissionsValidation = false;
        }

        if (needPermissionsValidation) {
            Transaction_->EnableAccessControlPreload();
        }

        queryContext->GetQuery()->WherePredicate = TExpressionList{std::move(filterExpression)};
        queryContext->GetQuery()->Limit = limit;
        queryContext->GetQuery()->Offset = offset;

        auto rowset = RunSelect(queryContext->Finalize(), fetcherContext.VersionedIOMode());
        auto rows = rowset->GetRows();

        if (syntheticLimit) {
            YT_VERIFY(limit);
            if (std::ssize(rows) >= *limit) {
                THROW_ERROR_EXCEPTION("Intermediate query result contains too many rows (>=%v)",
                    *limit);
            }
        }

        bool limitSatisfied = limit && (std::ssize(rows) < *limit);

        // Avoid instantiating objects unless needed.
        std::vector<TObject*> rowObjects;
        if (needPermissionsValidation || checkReadPermissions) {
            rowObjects = fetcherContext.GetObjects(rows);
        }

        YT_LOG_DEBUG("Prefetching results");

        if (computedFilterResult.FilterExpression) {
            for (auto row : rows) {
                for (const auto& fetcher : computedFilterFetchers) {
                    fetcher->Prefetch(row, /*object*/ nullptr);
                }
            }
        }

        if (Options_.FetchValues) {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSelectQueryExecutor::Prefetch"));
            PrefetchAttributeValues(rows, selectorFetchers);
        }
        if (needOrderedOutput) {
            if (rows.size() > 0) {
                for (const auto& fetcher : continuationTokenFetchers) {
                    // May be the wrong row in case of limited read.
                    fetcher->Prefetch(rows.Back(), /*object*/ nullptr);
                }
            }
        }

        {
            TTraceContextGuard guard(
                CreateTraceContextFromCurrent("NYT::NOrm::TSelectQueryExecutor::PrefetchTimestamps"));
            for (auto row : rows) {
                for (const auto& fetcher : timestampFetchers) {
                    fetcher->Prefetch(row);
                }
            }
        }

        YT_LOG_DEBUG("Fetching results");

        if (needPermissionsValidation) {
            ValidateObjectPermissions(rowObjects, permissions);
        }

        TSelectQueryResult result;
        std::optional<size_t> lastRowIndex;

        std::vector<bool> allowObjectsMask;
        if (checkReadPermissions) {
            allowObjectsMask = CheckAndFilterReadPermissions(rowObjects, pathsToCheckReadAccess, limitedRead);
        }

        NQuery::IFilterMatcherPtr matcher;
        if (computedFilterResult.FilterExpression) {
            auto filter = FormatExpression({computedFilterResult.FilterExpression});
            std::vector<NQuery::TTypedAttributePath> attributePaths;
            attributePaths.reserve(computedFilterResult.ResolvedAttributes.size());
            for (const auto& [path, resolvedAttribute] : Zip(computedFilterResult.FullPaths, computedFilterResult.ResolvedAttributes)) {
                auto type = resolvedAttribute.Attribute->TryAsScalar()
                    ? AttributeTypeToValueType(resolvedAttribute.Attribute
                        ->AsScalar()
                        ->GetType(resolvedAttribute.SuffixPath))
                    : EValueType::Any;
                attributePaths.push_back(NQuery::TTypedAttributePath{
                    .Path = path,
                    .Type = type
                });
            }
            matcher = NQuery::CreateFilterMatcher(
                filter,
                std::move(attributePaths));
            YT_LOG_DEBUG("Computed filter was built (Filter: %v)", filter);
        }

        std::vector<size_t> filteredRows;
        filteredRows.reserve(rows.size());

        {
            TTraceContextGuard guard(
                CreateTraceContextFromCurrent("NYT::NOrm::TSelectQueryExecutor::Fetch"));

            size_t batchSize = std::max<size_t>(
                1,
                Transaction_->GetConfig()->PartialResultBatchFraction * limit.value_or(rows.size()));
            int filteredRowsSize = 0;

            for (int rowIndex = 0; rowIndex < std::ssize(rows); ++rowIndex) {
                if (Options_.EnablePartialResult && rowIndex > 0 && CheckRequestRemainingTimeoutExpired()) {
                    lastRowIndex = rowIndex - 1;
                    break;
                }

                if (checkReadPermissions && !allowObjectsMask[rowIndex]) {
                    continue;
                }

                if (computedFilterResult.FilterExpression) {
                    std::vector<NQuery::TNonOwningAttributePayload> payloads;
                    std::vector<NYson::TYsonString> fetchedStrings;
                    payloads.reserve(computedFilterResult.ResolvedAttributes.size());
                    fetchedStrings.reserve(computedFilterResult.ResolvedAttributes.size());

                    NAttributes::TYsonStringBuilder helper;
                    auto consumer = helper.GetConsumer();

                    for (const auto& fetcher : computedFilterFetchers) {
                        fetcher->Fetch(rows[rowIndex], /*object*/ nullptr, consumer);
                        auto payload = helper.Flush();
                        fetchedStrings.push_back(payload);
                        payloads.push_back(fetchedStrings.back());
                    }
                    if (!matcher->Match(payloads).ValueOrThrow()) {
                        continue;
                    }
                }

                if (masterSideLimit && filteredRowsSize >= *masterSideLimit)
                {
                    // We're about to exceed the master side limit, so stop here, and use the previous
                    // row for the continuation token. Limit is nonzero, so index is nonzero.
                    lastRowIndex = rowIndex - 1;
                    // We know there are more rows to read.
                    limitSatisfied = false;
                    break;
                }

                filteredRows.push_back(rowIndex);
                ++filteredRowsSize;

                if (Options_.EnablePartialResult) {
                    if (filteredRows.size() >= batchSize) {
                        FetchFilteredRows(consumerGroup, filteredRows, rows, selectorFetchers, timestampFetchers);
                        filteredRows.clear();
                    }
                }
            }

            if (!filteredRows.empty()) {
                FetchFilteredRows(consumerGroup, filteredRows, rows, selectorFetchers, timestampFetchers);
            }
        }

        YT_LOG_DEBUG("Fetched results");

        // Continuation token does not make any sense without proper ordering.
        if (needOrderedOutput) {
            auto& newContinuationToken = result.ContinuationToken.emplace();
            if (rows.size() > 0) {
                InitializeSelectContinuationTokenVersion(newContinuationToken, Bootstrap_);

                if (!lastRowIndex) {
                    lastRowIndex = rows.size() - 1;
                }
                // Rely on rows ordered by continuation token first.
                auto lastRow = rows[*lastRowIndex];

                newContinuationToken.EvaluatedEntries.resize(continuationTokenFetchers.size());
                for (int index = 0; index < std::ssize(continuationTokenFetchers); ++index) {
                    auto& evaluatedEntry = newContinuationToken.EvaluatedEntries[index];
                    evaluatedEntry.AsNotEvaluated() = continuationTokenEntries[index];
                    evaluatedEntry.YsonValue = FetchYsonString(continuationTokenFetchers[index], /*object*/ nullptr, lastRow);
                }
            } else if (continuationToken) {
                newContinuationToken = *continuationToken;
            } else {
                InitializeSelectContinuationTokenVersion(newContinuationToken, Bootstrap_);
            }
        }

        result.LimitSatisfied = limitSatisfied;

        return result;
    }

    void PatchOptions(std::optional<TSelectObjectsContinuationToken> token, i64 masterSideLimit) override
    {
        Options_.ContinuationToken = std::move(token);
        Options_.MasterSideLimit = masterSideLimit;
    }

private:
    const TObjectTypeValue Type_;
    const std::optional<TObjectFilter> Filter_;
    const TAttributeSelector Selector_;
    const TObjectOrderBy OrderBy_;
    const std::optional<TIndex> Index_;

    TSelectQueryOptions Options_;

private:
    bool IsSchemaPermissionAllowedFor(
        const NYPath::TYPath& path,
        TAccessControlPermissionValue permission) const
    {
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        auto result = accessControlManager->CheckCachedPermission(
            GetAuthenticatedUserIdentity().User,
            TObjectTypeValues::Schema,
            TObjectKey(NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetTypeNameByValueOrCrash(Type_)),
            permission,
            path);
        return result.Action == NAccessControl::EAccessControlAction::Allow;
    }

    std::vector<bool> CheckAndFilterReadPermissions(
        std::vector<TObject*> objects,
        const THashSet<NYPath::TYPath>& pathsToCheckReadAccess,
        bool limitedRead) const
    {
        std::vector<TObjectPermission> requests;
        for (auto object : objects) {
            for (const auto& path : pathsToCheckReadAccess) {
                requests.push_back(TObjectPermission{
                    .Object = object,
                    .AttributePath = path,
                    .Permission = TAccessControlPermissionValues::Read,
                });
            }
        }

        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        std::vector<bool> result(objects.size(), true);
        if (limitedRead) {
            auto checkedPermissions = accessControlManager->CheckPermissions(
                GetAuthenticatedUserIdentity().User,
                requests);
            for (int index = 0; index < std::ssize(requests); ++index) {
                auto& resultSlot = result[index / pathsToCheckReadAccess.size()];
                resultSlot = resultSlot &&
                    (checkedPermissions[index].Action == NAccessControl::EAccessControlAction::Allow);
            }
        } else {
            accessControlManager->ValidatePermissions(requests);
        }

        return result;
    }

    void FetchFilteredRows(
        IAttributeValuesConsumerGroup* consumerGroup,
        const std::vector<size_t>& filteredRows,
        const TSharedRange<TUnversionedRow>& rows,
        std::span<IFetcherPtr> selectorFetchers,
        std::span<ITimestampFetcherPtr> timestampFetchers) const
    {
        auto consumers = consumerGroup->CreateConsumers(filteredRows.size());
        for (auto [rowIndex, consumer] : Zip(filteredRows, consumers)) {
            consumer->Initialize();
            if (Options_.FetchValues) {
                FillAttributeValues(
                    Options_.FetchRootObject,
                    Selector_,
                    consumer.get(),
                    rows[rowIndex],
                    selectorFetchers);
            }

            std::vector<TTimestamp> timestamps;
            timestamps.reserve(timestampFetchers.size());
            for (const auto& fetcher : timestampFetchers) {
                timestamps.push_back(fetcher->Fetch(rows[rowIndex]));
            }
            consumer->Finalize(std::move(timestamps));
        }
    }

    bool CheckRequestRemainingTimeoutExpired() const override
    {
        auto timeoutRemaining = Transaction_->GetRequestTimeoutRemaining();
        return timeoutRemaining && *timeoutRemaining < Transaction_->GetConfig()->PartialResultSelectTimeoutSlack;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISelectQueryExecutor> MakeSelectQueryExecutor(
    TTransactionPtr transaction,
    TObjectTypeValue type,
    const std::optional<TObjectFilter>& filter,
    const TAttributeSelector& selector,
    const TObjectOrderBy& orderBy,
    const TSelectQueryOptions& options,
    const std::optional<TIndex>& index)
{
    return std::make_unique<TSelectQueryExecutor>(
        transaction,
        type,
        filter,
        selector,
        orderBy,
        options,
        index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
