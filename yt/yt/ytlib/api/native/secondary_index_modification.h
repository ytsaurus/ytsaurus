#pragma once

#include "tablet_request_batcher.h"

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/dynamic_table_transaction.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISecondaryIndexModifier)

struct ISecondaryIndexModifier
    : public TRefCounted
{
    virtual TFuture<void> LookupRows() = 0;

    virtual TFuture<void> OnIndexModifications(std::function<void(
        NYPath::TYPath path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications)> enqueueModificationRequests) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISecondaryIndexModifier)

////////////////////////////////////////////////////////////////////////////////

ISecondaryIndexModifierPtr CreateSecondaryIndexModifier(
    ITransactionPtr transaction,
    NTabletClient::TTableMountInfoPtr tableMountInfo,
    std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
    TRange<TUnversionedSubmittedRow> mergedModifications,
    NQueryClient::IColumnEvaluatorCachePtr columnEvaluatorCache,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

// For testing purposes.

using TLookupSignature = TFuture<TSharedRange<NTableClient::TUnversionedRow>>(
    NYPath::TYPath path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<NTableClient::TLegacyKey> keys,
    TLookupRowsOptions options);

ISecondaryIndexModifierPtr CreateSecondaryIndexModifier(
    std::function<TLookupSignature> lookuper,
    NTabletClient::TTableMountInfoPtr tableMountInfo,
    std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
    TRange<TUnversionedSubmittedRow> mergedModifications);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
