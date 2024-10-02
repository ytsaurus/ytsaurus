#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/misc/objects_holder.h>

#include <yt/yt/client/table_client/versioned_io_options.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

inline const TString PrimaryTableAlias("p");
inline const TString AnnotationsTableAliasPrefix("c");
inline const TString IndexTableAlias("i");

////////////////////////////////////////////////////////////////////////////////

void EnsureNonEmptySelectExpressions(
    TObjectsHolder* holder,
    NYT::NQueryClient::NAst::TQuery* query);

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TReferenceExpressionPtr BuildPrimaryTableFieldReference(
    TObjectsHolder* holder,
    const TDBField* field);

NQueryClient::NAst::TExpressionPtr BuildObjectFilterByNullColumn(TObjectsHolder* holder, const TDBField* field);

////////////////////////////////////////////////////////////////////////////////

template <class TSelectContinuationToken>
void InitializeSelectContinuationTokenVersion(
    TSelectContinuationToken& token,
    NMaster::IBootstrap* bootstrap);

template <class TSelectContinuationToken>
void ValidateSelectContinuationTokenVersion(
    const TSelectContinuationToken& token,
    NMaster::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

std::vector<TResolveAttributeResult> ResolveAttributes(
    const IObjectTypeHandler* typeHandler,
    const TAttributeSelector& selector,
    TAttributePermissionsCollector& permissions);

std::vector<IFetcherPtr> BuildAttributeFetchers(
    TFetcherContext* fetcherContext,
    const std::vector<TResolveAttributeResult>& resolveResults,
    bool fetchRootObject,
    EFetchRootOptimizationLevel level);

std::vector<ITimestampFetcherPtr> BuildTimestampFetchers(
    TFetcherContext* fetcherContext,
    bool fetchTimestamps,
    const std::vector<TResolveAttributeResult>& resolvedAttributes,
    bool versionedSelectEnabled);

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutorBase
{
public:
    explicit TQueryExecutorBase(TTransactionPtr transaction);

protected:
    const NLogging::TLogger Logger;
    NMaster::IBootstrap* const Bootstrap_;
    const TTransactionPtr Transaction_;

    void FillAttributeValues(
        bool fetchRootObject,
        const TAttributeSelector& selector,
        IAttributeValuesConsumer* consumer,
        NTableClient::TUnversionedRow row,
        std::span<IFetcherPtr> fetchers,
        TObject* object = nullptr) const;

    void FillAttributeTimestamps(
        std::vector<NTableClient::TTimestamp>* timestamps,
        TObject* object,
        const std::vector<TResolveAttributeResult>& resolveResults) const;

    NApi::IUnversionedRowsetPtr RunSelect(
        std::string queryString,
        NTableClient::EVersionedIOMode versionedIOMode = NTableClient::EVersionedIOMode::Default) const;

    TSharedRange<NTableClient::TUnversionedRow> RunLookup(
        const TDBTable* table,
        TRange<TObjectKey> keys,
        TRange<const TDBField*> fields) const;

    void PrefetchAttributeValues(
        TRange<NTableClient::TUnversionedRow> rows,
        const std::vector<IFetcherPtr>& fetchers) const;

    void PrefetchAttributeTimestamps(
        const std::vector<TObject*>& objects,
        const std::vector<TResolveAttributeResult>& resolveResults) const;

    void ValidateObjectPermissions(
        const std::vector<TObject*>& objects,
        const TAttributePermissionsCollector& permissions) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define QUERY_EXECUTOR_HELPERS_INL_H_
#include "query_executor_helpers-inl.h"
#undef QUERY_EXECUTOR_HELPERS_INL_H_
