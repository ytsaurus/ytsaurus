#pragma once

#include "tablet_request_batcher.h"

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/api/dynamic_table_transaction.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TSecondaryIndexModifier
{
public:
    TSecondaryIndexModifier(
        NTabletClient::TTableMountInfoPtr tableMountInfo,
        std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
        TRange<TUnversionedSubmittedRow> mergedModifications,
        NQueryClient::IExpressionEvaluatorCachePtr expressionEvaluatorCache,
        NLogging::TLogger logger);

    TFuture<void> LookupRows(ITransaction* transaction);

    void OnIndexModifications(std::function<void(
        NYPath::TYPath path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications)> enqueueModificationRequests);

private:
    using TInitialRowMap = THashMap<NTableClient::TKey, NTableClient::TUnversionedRow>;
    using TResultingRowMap = THashMap<NTableClient::TKey, NTableClient::TMutableUnversionedRow>;

    struct TIndexDescriptor
    {
        NTabletClient::ESecondaryIndexKind Kind;
        std::optional<int> UnfoldedColumnPosition;
        std::unique_ptr<NQueryClient::TParsedSource> Predicate;
    };

    const NTabletClient::TTableMountInfoPtr TableMountInfo_;
    const std::vector<NTabletClient::TTableMountInfoPtr> IndexInfos_;
    const NTableClient::TNameTablePtr NameTable_;
    const TRange<TUnversionedSubmittedRow> MergedModifications_;
    const NQueryClient::IExpressionEvaluatorCachePtr ExpressionEvaluatorCache_;
    const NTableClient::TRowBufferPtr RowBuffer_;

    const NLogging::TLogger Logger;

    std::vector<TIndexDescriptor> IndexDescriptors_;
    std::vector<int> UnfoldedColumnIndices_;

    NTableClient::TNameTableToSchemaIdMapping ResultingRowMapping_;
    std::vector<int> PositionToIdMapping_;
    NTableClient::TTableSchemaPtr ResultingSchema_;

    TInitialRowMap InitialRowMap_;
    TResultingRowMap ResultingRowMap_;

    void SetInitialAndResultingRows(TSharedRange<NTableClient::TUnversionedRow> lookedUpRows);

    TSharedRange<TRowModification> ProduceModificationsForIndex(int index);

    TSharedRange<TRowModification> ProduceFullSyncModifications(
        const NTableClient::TNameTableToSchemaIdMapping& indexIdMapping,
        const NTableClient::TNameTableToSchemaIdMapping& keyIndexIdMapping,
        const NTableClient::TTableSchema& indexSchema,
        std::function<bool(NTableClient::TUnversionedRow)> predicate,
        const std::optional<NTableClient::TUnversionedValue>& empty) const;

    TSharedRange<TRowModification> ProduceUnfoldingModifications(
        const NTableClient::TNameTableToSchemaIdMapping& indexIdMapping,
        const NTableClient::TNameTableToSchemaIdMapping& keyIndexIdMapping,
        const NTableClient::TTableSchema& indexSchema,
        std::function<bool(NTableClient::TUnversionedRow)> predicate,
        const std::optional<NTableClient::TUnversionedValue>& empty,
        int unfoldedKeyPosition) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
