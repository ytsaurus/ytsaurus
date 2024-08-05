#pragma once

#include "tablet_request_batcher.h"

#include <yt/yt/library/query/base/query_preparer.h>

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
    NQueryClient::IExpressionEvaluatorCachePtr expressionEvaluatorCache,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
