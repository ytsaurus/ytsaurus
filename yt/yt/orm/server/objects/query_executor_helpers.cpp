#include "query_executor_helpers.h"

#include "fetchers.h"
#include "object_reflection.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NAccessControl;
using namespace NQueryClient::NAst;
using namespace NTableClient;

using NApi::IUnversionedRowsetPtr;
using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

void EnsureNonEmptySelectExpressions(
    TObjectsHolder* holder,
    NYT::NQueryClient::NAst::TQuery* query)
{
    if (query->SelectExprs->empty()) {
        static const auto DummyExpr = holder->New<TLiteralExpression>(
            TSourceLocation(),
            TLiteralValue(false));
        query->SelectExprs->push_back(DummyExpr);
    }
}

////////////////////////////////////////////////////////////////////////////////

TReferenceExpressionPtr BuildPrimaryTableFieldReference(TObjectsHolder* holder, const TDBField* field)
{
    return holder->New<TReferenceExpression>(
        TSourceLocation(),
        TReference(field->Name, PrimaryTableAlias));
}

TExpressionPtr BuildObjectFilterByNullColumn(TObjectsHolder* holder, const TDBField* field)
{
    return holder->New<TFunctionExpression>(
        TSourceLocation(),
        "is_null",
        TExpressionList{
            BuildPrimaryTableFieldReference(holder, field)
        });
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TResolveAttributeResult> ResolveAttributes(
    const IObjectTypeHandler* typeHandler,
    const TAttributeSelector& selector,
    TAttributePermissionsCollector& permissions)
{
    std::vector<TResolveAttributeResult> results;
    results.reserve(selector.Paths.size());
    for (const auto& path : selector.Paths) {
        results.push_back(ResolveAttribute(typeHandler, path, permissions));
    }
    return results;
}

std::vector<IFetcherPtr> BuildAttributeFetchers(
    TFetcherContext* fetcherContext,
    const std::vector<TResolveAttributeResult>& resolveResults,
    bool fetchRootObject,
    EFetchRootOptimizationLevel level)
{
    std::vector<IFetcherPtr> fetchers;
    if (fetchRootObject) {
        fetchers.push_back(MakeRootFetcher(resolveResults, fetcherContext, level));
    } else {
        // Reserve one extra element for potential uuid load.
        fetchers.reserve(resolveResults.size() + 1);
        for (const auto& resolveResult : resolveResults) {
            fetchers.push_back(MakeSelectorFetcher(resolveResult, fetcherContext));
        }
    }

    return fetchers;
}

std::vector<ITimestampFetcherPtr> BuildTimestampFetchers(
    TFetcherContext* fetcherContext,
    bool fetchTimestamps,
    const std::vector<TResolveAttributeResult>& resolvedAttributes,
    bool versionedSelectEnabled)
{
    std::vector<ITimestampFetcherPtr> timestampFetchers;
    if (fetchTimestamps) {
        timestampFetchers.reserve(resolvedAttributes.size());
        for (const auto& resolveResult : resolvedAttributes) {
            timestampFetchers.push_back(MakeTimestampFetcher(
                resolveResult,
                fetcherContext,
                versionedSelectEnabled));
        }
    }
    return timestampFetchers;
}

////////////////////////////////////////////////////////////////////////////////

TQueryExecutorBase::TQueryExecutorBase(
    TTransactionPtr transaction)
    : Logger(MakeTransactionLogger(transaction->GetId()))
    , Bootstrap_(transaction->GetBootstrap())
    , Transaction_(std::move(transaction))
{ }

void TQueryExecutorBase::FillAttributeValues(
    bool fetchRootObject,
    const TAttributeSelector& selector,
    IAttributeValuesConsumer* consumer,
    TUnversionedRow row,
    std::span<IFetcherPtr> fetchers,
    TObject* object) const
{
    if (fetchRootObject) {
        YT_VERIFY(std::ssize(fetchers) == 1);
        fetchers[0]->Fetch(row, object, consumer->OnValueBegin(/*selector*/ {}));
        consumer->OnValueEnd();

    } else {
        YT_VERIFY(std::ssize(selector.Paths) == std::ssize(fetchers));
        for (const auto& [fetcher, selectorPath] : Zip(fetchers, selector.Paths)) {
            auto ysonConsumer = consumer->OnValueBegin(selectorPath);
            fetcher->Fetch(row, object, ysonConsumer);
            consumer->OnValueEnd();
        }
    }
}

void TQueryExecutorBase::FillAttributeTimestamps(
    std::vector<NTableClient::TTimestamp>* timestamps,
    TObject* object,
    const std::vector<TResolveAttributeResult>& resolveResults) const
{
    timestamps->reserve(resolveResults.size());
    for (const auto& resolveResult : resolveResults) {
        timestamps->push_back(FetchAttributeTimestamp(resolveResult, object));
    }
}

IUnversionedRowsetPtr TQueryExecutorBase::RunSelect(
    std::string queryString,
    NTableClient::EVersionedIOMode versionedIOMode) const
{
    IUnversionedRowsetPtr rowset;
    Transaction_->GetSession()->ScheduleLoad(
        [queryString = std::move(queryString), &rowset, versionedIOMode] (ILoadContext* context) {
            context->ScheduleSelect(
                queryString,
                [&rowset] (const IUnversionedRowsetPtr& selectedRowset) {
                    rowset = selectedRowset;
                },
                versionedIOMode);
        });
    Transaction_->GetSession()->FlushLoads();
    return rowset;
}

TSharedRange<TUnversionedRow> TQueryExecutorBase::RunLookup(
    const TDBTable* table,
    TRange<TObjectKey> keys,
    TRange<const TDBField*> fields) const
{
    TSharedRange<TUnversionedRow> rows;
    Transaction_->GetSession()->ScheduleLoad(
        [&rows, table, keys, fields] (ILoadContext* context) {
            context->ScheduleLookup(
                table,
                keys,
                fields,
                [&rows] (TSharedRange<TUnversionedRow> lookedUpRows, const TDynBitMap&) {
                    rows = std::move(lookedUpRows);
                });
        });
    Transaction_->GetSession()->FlushLoads();
    return rows;
}

void TQueryExecutorBase::PrefetchAttributeValues(
    TRange<TUnversionedRow> rows,
    const std::vector<IFetcherPtr>& fetchers) const
{
    for (auto row : rows) {
        for (const auto& fetcher : fetchers) {
            fetcher->Prefetch(row, /*object*/ nullptr);
        }
    }
}

void TQueryExecutorBase::PrefetchAttributeTimestamps(
    const std::vector<TObject*>& objects,
    const std::vector<TResolveAttributeResult>& resolveResults) const
{
    for (auto* object : objects) {
        for (const auto& resolveResult : resolveResults) {
            PrefetchAttributeTimestamp(resolveResult, object);
        }
    }
}

void TQueryExecutorBase::ValidateObjectPermissions(
    const std::vector<TObject*>& objects,
    const TAttributePermissionsCollector& permissions) const
{
    std::vector<TObjectPermission> requests;
    for (auto* object : objects) {
        for (auto permission : permissions.GetReadPermissions()) {
            requests.push_back(TObjectPermission{
                .Object = object,
                .AttributePath = "",
                .Permission = permission,
            });
        }
    }
    Bootstrap_->GetAccessControlManager()->ValidatePermissions(requests);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
