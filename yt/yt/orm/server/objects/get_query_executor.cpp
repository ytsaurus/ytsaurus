#include "get_query_executor.h"

#include "attribute_matcher.h"
#include "attribute_values_consumer.h"
#include "fetchers.h"
#include "object.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "query_executor_helpers.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NTracing;

using namespace NAccessControl;

using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TGetQueryOptions& options,
    TStringBuf /*format*/)
{
    builder->AppendFormat("{"
        "NonexistentObjectPolicy: %v, "
        "FetchValues: %v, "
        "FetchTimestamps: %v, "
        "FetchRootObject: %v, "
        "CheckReadPermissions: %v, "
        "ReadUncommittedChanges: %v}",
        options.NonexistentObjectPolicy,
        options.FetchValues,
        options.FetchTimestamps,
        options.FetchRootObject,
        options.CheckReadPermissions,
        options.ReadUncommittedChanges);
}

////////////////////////////////////////////////////////////////////////////////

class TGetQueryExecutor
    : public IGetQueryExecutor
    , public TQueryExecutorBase
{
public:
    TGetQueryExecutor(
        TTransactionPtr transaction,
        TObjectTypeValue type,
        const TAttributeSelector& selector,
        const std::vector<TKeyAttributeMatches>& matches,
        const TGetQueryOptions& options)
        : TQueryExecutorBase(std::move(transaction))
        , Type_(type)
        , Selector_(selector)
        , Matches_(matches)
        , Options_(options)
    { }

    void ExecuteConsuming(IAttributeValuesConsumerGroup* consumerGroup) && override
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TGetQueryExecutor::ExecuteConsuming"));

        if (Options_.CheckReadPermissions) {
            Transaction_->EnableAccessControlPreload();
        }

        bool matchUuids = false;
        std::vector<TObject*> requestedObjects;
        requestedObjects.reserve(Matches_.size());
        for (const auto& match : Matches_) {
            requestedObjects.push_back(Transaction_->GetObject(Type_, match.Key, match.ParentKey));
            if (match.Uuid) {
                matchUuids = true;
            }
        }

        if (Options_.NonexistentObjectPolicy == ENonexistentObjectPolicy::Fail) {
            for (auto* object : requestedObjects) {
                object->ValidateExists();
            }
        }

        if (Options_.CheckReadPermissions) {
            CheckReadPermissions(requestedObjects, Selector_);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(Type_);
        auto* primaryTable = typeHandler->GetTable();
        const auto& tableKeyFields = primaryTable->GetKeyFields(/*filterEvaluatedFields*/ true);

        TObjectsHolder holder;
        TExpressionList keyColumnsExpr;
        for (auto* field : tableKeyFields) {
            keyColumnsExpr.push_back(holder.New<TReferenceExpression>(
                TSourceLocation(),
                field->Name,
                PrimaryTableAlias));
        }

        std::vector<const TKeyAttributeMatches*> filteredMatches;
        filteredMatches.reserve(Matches_.size());

        std::vector<TObject*> preexistingObjects;
        std::vector<TObject*> uncommittedObjects;
        std::vector<int> uncommittedObjectIndexes;
        THashMap<TObjectKey, std::vector<size_t>> objectKeyToConsumerIndexes;
        preexistingObjects.reserve(requestedObjects.size());

        for (int index = 0; index < std::ssize(requestedObjects); ++index) {
            auto& object = requestedObjects[index];
            bool exists = Options_.ReadUncommittedChanges ? object->DoesExist() : object->DidExist();

            if (!exists && Options_.NonexistentObjectPolicy != ENonexistentObjectPolicy::Ignore) {
                continue;
            }

            size_t consumerIndex = filteredMatches.size();
            filteredMatches.push_back(&Matches_[index]);

            if (exists) {
                if (!object->DidExist() || object->GetPredecessor()) {
                    uncommittedObjects.push_back(object);
                    uncommittedObjectIndexes.push_back(consumerIndex);
                } else {
                    auto& indexes = objectKeyToConsumerIndexes[Matches_[index].Key];
                    if (indexes.empty()) {
                        // Do not fetch duplicate objects.
                        preexistingObjects.push_back(object);
                    }
                    indexes.push_back(consumerIndex);
                }
            }
        }

        auto consumers = consumerGroup->CreateConsumers(filteredMatches.size());
        bool noRowObjects = preexistingObjects.empty();
        if (noRowObjects && uncommittedObjects.empty()) {
            return;
        }

        auto queryContext = MakeQueryContext(Bootstrap_, Type_, Transaction_->GetSession());

        TAttributePermissionsCollector permissions;
        auto resolveResults = ResolveAttributes(
            typeHandler,
            Selector_,
            permissions);

        TFetcherContext fetcherContext(queryContext.get(), Transaction_.Get(), Options_.ReadUncommittedChanges);
        fetcherContext.RegisterKeyAndParentKeyFields(typeHandler);

        auto fetchers = BuildAttributeFetchers(
            &fetcherContext,
            resolveResults,
            Options_.FetchRootObject,
            Transaction_->GetConfig()->FetchRootOptimizationLevel);

        IFetcherPtr uuidFetcher;
        if (matchUuids) {
            fetchers.push_back(
                MakeSelectorFetcher(ResolveAttribute(typeHandler, "/meta/uuid"),
                &fetcherContext));
            uuidFetcher = fetchers.back();
        }

        auto consumedFetchers = std::span<IFetcherPtr>(fetchers);
        if (matchUuids) {
            consumedFetchers = consumedFetchers.subspan(0, consumedFetchers.size() - 1);
        }

        TSharedRange<NQueryClient::TRow> rows;

        if (!noRowObjects) {
            queryContext->GetQuery()->SelectExprs = fetcherContext.GetSelectExpressions();
            EnsureNonEmptySelectExpressions(queryContext.get(), queryContext->GetQuery());

            auto lookupFields = Transaction_->GetConfig()->CanUseLookupForGetObjects
                ? fetcherContext.ExtractLookupFields(primaryTable)
                : std::nullopt;

            YT_LOG_DEBUG("Getting objects (Attributes: %v, UseLookup: %v)",
                MakeShrunkFormattableView(
                    Matches_,
                    TDefaultFormatter(),
                    Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests),
                lookupFields.has_value());

            if (lookupFields) {
                std::vector<TObjectKey> keys;
                keys.reserve(preexistingObjects.size());

                for (auto* object : preexistingObjects) {
                    keys.push_back(typeHandler->GetObjectTableKey(object));
                }

                rows = RunLookup(primaryTable, keys, *lookupFields);
            } else {
                TLiteralValueTupleList keyTupleList;
                keyTupleList.reserve(preexistingObjects.size());

                for (auto* object : preexistingObjects) {
                    auto& tuple = keyTupleList.emplace_back();
                    auto tableKey = typeHandler->GetObjectTableKey(object);
                    for (auto field : tableKey) {
                        tuple.push_back(field.AsLiteralValue());
                    }
                }

                auto inExpression = holder.New<TInExpression>(
                    TSourceLocation(),
                    std::move(keyColumnsExpr),
                    std::move(keyTupleList));

                queryContext->GetQuery()->WherePredicate = TExpressionList{std::move(inExpression)};

                auto queryString = queryContext->Finalize();
                auto rowset = RunSelect(std::move(queryString));
                rows = rowset->GetRows();
            }
        }

        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TGetQueryExecutor::Prefetch"));
            YT_LOG_DEBUG("Prefetching results");
            PrefetchAttributeValues(rows, fetchers);
            for (auto* object : uncommittedObjects) {
                for (const auto& fetcher : fetchers) {
                    fetcher->Prefetch(TUnversionedRow{}, object);
                }
            }
        }

        auto rowObjects = fetcherContext.GetObjects(rows);

        ValidateObjectPermissions(rowObjects, permissions);

        if (Options_.FetchTimestamps) {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TGetQueryExecutor::PrefetchTimestamps"));
            PrefetchAttributeTimestamps(rowObjects, resolveResults);
        }

        TTraceContextGuard fetchGuard(CreateTraceContextFromCurrent("NYT::NOrm::TGetQueryExecutor::Fetch"));
        YT_LOG_DEBUG("Fetching results");

        TYsonStringBuilder uuidBuilder(NYson::EYsonFormat::Text);

        auto fetch = [&] (
            const TKeyAttributeMatches& match,
            IAttributeValuesConsumer* consumer,
            TObject* object,
            TUnversionedRow row)
        {
            consumer->Initialize();

            if (Options_.FetchValues) {
                FillAttributeValues(
                    Options_.FetchRootObject,
                    Selector_,
                    consumer,
                    row,
                    consumedFetchers,
                    object);
            }

            std::vector<TTimestamp> timestamps;
            if (Options_.FetchTimestamps) {
                FillAttributeTimestamps(&timestamps, object, resolveResults);
            }

            consumer->Finalize(std::move(timestamps));

            if (match.Uuid) {
                uuidFetcher->Fetch(row, object, uuidBuilder.GetConsumer());
                auto uuid = NYTree::ConvertTo<TString>(uuidBuilder.Flush());
                THROW_ERROR_EXCEPTION_UNLESS(match.Uuid == uuid,
                    NClient::EErrorCode::InvalidRequestArguments,
                    "Requested uuid %v does not match object uuid %v",
                    match.Uuid,
                    uuid);
            }
        };

        for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
            auto row = rows[rowIndex];
            auto* object = rowObjects[rowIndex];

            const auto& indexes = GetOrCrash(objectKeyToConsumerIndexes, fetcherContext.GetObjectKey(row));
            YT_VERIFY(!indexes.empty());

            for (const auto consumerIndex : indexes) {
                fetch(*filteredMatches[consumerIndex], consumers[consumerIndex].get(), object, row);
            }
        }

        for (auto [object, index] : Zip(uncommittedObjects, uncommittedObjectIndexes)) {
            fetch(*filteredMatches[index], consumers[index].get(), object, TUnversionedRow{});
        }

        YT_LOG_DEBUG("Fetched results");
    }

private:
    const TObjectTypeValue Type_;
    const TAttributeSelector Selector_;
    const std::vector<TKeyAttributeMatches> Matches_;
    const TGetQueryOptions Options_;

private:
    void CheckReadPermissions(
        std::vector<TObject*> objects,
        const TAttributeSelector& selector)
    {
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        static const std::vector<NYPath::TYPath> DefaultPaths = {""};
        const auto& paths = selector.Paths.empty() ? DefaultPaths : selector.Paths;
        std::vector<TObjectPermission> requests;
        for (auto* object : objects) {
            for (const auto& path : paths) {
                requests.push_back(TObjectPermission{
                    .Object = object,
                    .AttributePath = path,
                    .Permission = TAccessControlPermissionValues::Read,
                });
            }
        }
        accessControlManager->ValidatePermissions(requests);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGetQueryExecutor> MakeGetQueryExecutor(
    TTransactionPtr transaction,
    TObjectTypeValue type,
    const TAttributeSelector& selector,
    const std::vector<TKeyAttributeMatches>& matches,
    const TGetQueryOptions& options)
{
    return std::make_unique<TGetQueryExecutor>(
        std::move(transaction),
        type,
        selector,
        matches,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
