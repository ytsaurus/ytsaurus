#include "tablet_helpers.h"
#include "secondary_index_modification.h"
#include "transaction.h"

#include <yt/yt/library/query/secondary_index/schema.h>

#include <yt/yt/library/query/base/ast_visitors.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/key.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NLogging;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TSecondaryIndexModificationsBufferTag { };

struct TIndexDescriptor
{
    NTabletClient::ESecondaryIndexKind Kind;
    std::optional<TUnfoldedColumns> UnfoldedColumns;
    std::optional<int> PredicatePosition;
    std::vector<int> AggregateColumnPositions;
};

struct TEvaluatedColumn
{
    std::string Name;
    std::string Expression;
    TLogicalTypePtr LogicalType;
};

using NAst::TReferenceHarvester;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSecondaryIndexModifier)

class TSecondaryIndexModifier
    : public ISecondaryIndexModifier
{
public:
    TSecondaryIndexModifier(
        std::function<TLookupSignature> lookuper,
        NTabletClient::TTableMountInfoPtr tableMountInfo,
        std::vector<NTabletClient::TTableMountInfoPtr> indexTableMountInfos,
        TRange<TUnversionedSubmittedRow> mergedModifications,
        NQueryClient::IColumnEvaluatorCachePtr columnEvaluatorCache,
        NLogging::TLogger logger);

    TFuture<void> LookupRows() override;

    TFuture<void> OnIndexModifications(std::function<void(
        NYPath::TYPath path,
        TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications)> enqueueModificationRequests) const override;

private:
    using TInitialRowMap = THashMap<TKey, TMutableUnversionedRow>;
    using TResultingRowMap = THashMap<TKey, TMutableUnversionedRow>;
    using TIndexKeyToTableKeyMap = THashMap<TUnversionedRow, TKey>;

    const std::function<TLookupSignature> Lookuper_;
    const NTabletClient::TTableMountInfoPtr TableMountInfo_;
    const std::vector<NTabletClient::TTableMountInfoPtr> IndexTableMountInfos_;
    const TNameTablePtr NameTable_;
    const TRange<TUnversionedSubmittedRow> MergedModifications_;
    const NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const TRowBufferPtr RowBuffer_;

    const NLogging::TLogger Logger;

    std::vector<TIndexDescriptor> IndexDescriptors_;

    TNameTableToSchemaIdMapping ResultingRowMapping_;
    std::vector<int> PositionToIdMapping_;
    int FirstEvaluatedColumnPosition_;
    std::vector<TEvaluatedColumn> EvaluatedColumnsSchema_;
    TTableSchemaPtr ResultingSchema_;

    TInitialRowMap InitialRowMap_;
    TResultingRowMap ResultingRowMap_;

    // We only store this range to keep ownership of the underlying row data.
    // Rows themselves are not used.
    TSharedRange<TUnversionedRow> LookedUpRows_;

    bool CanSkipLookup_ = false;

    void SetInitialAndResultingRows(TSharedRange<TUnversionedRow> lookedUpRows);

    void InitializeMapping(std::vector<int> tableColumnIds);

    TFuture<TSharedRange<TRowModification>> ProduceModificationsForIndex(int index) const;

    TFuture<TSharedRange<TRowModification>> ProduceFullSyncModifications(
        TNameTableToSchemaIdMapping indexIdMapping,
        TNameTableToSchemaIdMapping keyIndexIdMapping,
        TTableSchemaPtr indexSchema,
        std::optional<int> predicatePosition,
        std::optional<TUnversionedValue> empty) const;

    TFuture<TSharedRange<TRowModification>> ProduceUnfoldingModifications(
        TNameTableToSchemaIdMapping indexIdMapping,
        TNameTableToSchemaIdMapping keyIndexIdMapping,
        TTableSchemaPtr indexSchema,
        std::optional<int> predicatePosition,
        std::optional<TUnversionedValue> empty,
        const TUnfoldedColumns& unfoldedColumns,
        const std::vector<int>& aggregatePositions) const;

    TFuture<TSharedRange<TRowModification>> ProduceUniqueModifications(
        const NYPath::TYPath& uniqueIndexPath,
        TNameTableToSchemaIdMapping indexIdMapping,
        TNameTableToSchemaIdMapping keyIndexIdMapping,
        TTableSchemaPtr indexSchema,
        std::optional<int> predicatePosition,
        std::optional<TUnversionedValue> empty) const;

    TFuture<void> ValidateUniqueness(
        const NYPath::TYPath& uniqueIndexPath,
        const TNameTableToSchemaIdMapping& indexIdMapping,
        const TNameTableToSchemaIdMapping& keyIndexIdMapping,
        const TTableSchema& indexWriteSchema,
        std::optional<int> predicatePosition) const;

    bool IsPredicateGood(std::optional<int> predicatePosition, TUnversionedRow row) const;

    static bool IsNotValueColumn(const TTableSchema& tableSchema, TStringBuf columnName);
    static bool AreIndexColumnsDependentOnlyOnKeyColumns(
        const TTableSchema& tableSchema,
        const TTableSchema& indexTableSchema,
        const std::optional<TUnfoldedColumns>& unfoldedColumns);
};

DEFINE_REFCOUNTED_TYPE(TSecondaryIndexModifier)

////////////////////////////////////////////////////////////////////////////////

TSecondaryIndexModifier::TSecondaryIndexModifier(
    std::function<TLookupSignature> lookuper,
    TTableMountInfoPtr tableMountInfo,
    std::vector<TTableMountInfoPtr> indexTableMountInfos,
    TRange<TUnversionedSubmittedRow> mergedModifications,
    IColumnEvaluatorCachePtr columnEvaluatorCache,
    TLogger logger)
    : Lookuper_(std::move(lookuper))
    , TableMountInfo_(std::move(tableMountInfo))
    , IndexTableMountInfos_(std::move(indexTableMountInfos))
    , NameTable_(TNameTable::FromSchema(*TableMountInfo_->Schemas[ETableSchemaKind::Primary]))
    , MergedModifications_(std::move(mergedModifications))
    , ColumnEvaluatorCache_(std::move(columnEvaluatorCache))
    , RowBuffer_(New<TRowBuffer>(TSecondaryIndexModificationsBufferTag{}))
    , Logger(std::move(logger))
{
    YT_VERIFY(TableMountInfo_->Indices.size() == IndexTableMountInfos_.size());

    YT_LOG_DEBUG("Building secondary index modifications (TablePath: %v, Indices: [%v])",
        TableMountInfo_->Path,
        MakeFormattableView(
            TableMountInfo_->Indices,
            [] (auto* builder, const auto& indexInfo) {
                builder->AppendFormat("(%v: %Qlv)", indexInfo.TableId, indexInfo.Kind);
            }));

    const auto& tableSchema = TableMountInfo_->Schemas[ETableSchemaKind::Primary];

    std::vector<int> tableColumnIds;
    for (const auto& keyColumn : tableSchema->Columns()) {
        if (!keyColumn.SortOrder()) {
            break;
        }
        tableColumnIds.push_back(NameTable_->GetId(keyColumn.Name()));
    }

    CanSkipLookup_ = true;

    TColumnSet evaluatedExpressionsColumns;
    IndexDescriptors_.resize(IndexTableMountInfos_.size());
    THashSet<TStringBuf> accountedEvalutedIndexColumns;
    for (int index = 0; index < std::ssize(IndexTableMountInfos_); ++index) {
        const auto& indexMeta = TableMountInfo_->Indices[index];
        const auto& indexTableMountInfo = IndexTableMountInfos_[index];
        const auto& indexSchema = *indexTableMountInfo->Schemas[ETableSchemaKind::Primary];

        auto& descriptor = IndexDescriptors_[index];
        descriptor.Kind = indexMeta.Kind;

        if (const auto& predicate = indexMeta.Predicate) {
            auto parsedSource = ParseSource(*predicate, EParseMode::Expression);
            TReferenceHarvester(&evaluatedExpressionsColumns)
                .Visit(std::get<NAst::TExpressionPtr>(parsedSource->AstHead.Ast));

            descriptor.PredicatePosition = std::ssize(EvaluatedColumnsSchema_);
            EvaluatedColumnsSchema_.push_back(TEvaluatedColumn{
                .Name = Format("$predicate_%v", index),
                .Expression = *predicate,
                .LogicalType = SimpleLogicalType(ESimpleLogicalValueType::Boolean),
            });
        }

        if (indexMeta.EvaluatedColumnsSchema) {
            for (const auto& column : indexMeta.EvaluatedColumnsSchema->Columns()) {
                auto [_, inserted] = accountedEvalutedIndexColumns.insert(column.Name());
                if (!inserted) {
                    continue;
                }

                YT_VERIFY(column.Expression());

                auto parsedSource = ParseSource(*column.Expression(), EParseMode::Expression);
                TReferenceHarvester(&evaluatedExpressionsColumns)
                    .Visit(std::get<NAst::TExpressionPtr>(parsedSource->AstHead.Ast));

                EvaluatedColumnsSchema_.push_back(TEvaluatedColumn{
                    .Name = column.Name(),
                    .Expression = *column.Expression(),
                    .LogicalType = column.LogicalType(),
                });
            }
        }

        ValidateIndexSchema(
            descriptor.Kind,
            *tableSchema,
            indexSchema,
            indexMeta.Predicate,
            indexMeta.EvaluatedColumnsSchema,
            indexMeta.UnfoldedColumns);

        descriptor.UnfoldedColumns = indexMeta.UnfoldedColumns;

        for (const auto& column : indexSchema.Columns()) {
            auto tableColumnName = TStringBuf(column.Name());
            if (indexMeta.UnfoldedColumns && indexMeta.UnfoldedColumns->IndexColumn == tableColumnName) {
                NameTable_->GetIdOrRegisterName(tableColumnName);
                tableColumnName = indexMeta.UnfoldedColumns->TableColumn;
            }
            auto id = NameTable_->GetIdOrRegisterName(tableColumnName);
            if (tableSchema->FindColumn(tableColumnName)) {
                tableColumnIds.push_back(id);
            }

            if (column.Aggregate()) {
                // NB(sabdenovch): Ids will be changed into positions later.
                descriptor.AggregateColumnPositions.push_back(id);
            }
        }

        CanSkipLookup_ = CanSkipLookup_ && AreIndexColumnsDependentOnlyOnKeyColumns(
            *tableSchema,
            indexSchema,
            indexMeta.UnfoldedColumns);
    }

    for (const auto& column : evaluatedExpressionsColumns) {
        tableColumnIds.push_back(NameTable_->GetId(column));
    }

    InitializeMapping(std::move(tableColumnIds));

    for (auto& descriptor : IndexDescriptors_) {
        std::vector<int> positions;
        positions.reserve(descriptor.AggregateColumnPositions.size());
        for (auto id : descriptor.AggregateColumnPositions) {
            positions.push_back(ResultingRowMapping_[id]);
        }
        descriptor.AggregateColumnPositions = std::move(positions);
    }

    std::vector<TColumnSchema> resultingColumns;
    resultingColumns.reserve(PositionToIdMapping_.size());
    for (int position = 0; position < FirstEvaluatedColumnPosition_; ++position) {
        resultingColumns.push_back(TableMountInfo_
            ->Schemas[ETableSchemaKind::Primary]
            ->GetColumn(NameTable_->GetName(PositionToIdMapping_[position])));
    }
    for (int position = FirstEvaluatedColumnPosition_; position < std::ssize(PositionToIdMapping_); ++position) {
        const auto& evaluatedColumn = EvaluatedColumnsSchema_[position - FirstEvaluatedColumnPosition_];
        resultingColumns.push_back(TColumnSchema(evaluatedColumn.Name, evaluatedColumn.LogicalType)
            .SetExpression(evaluatedColumn.Expression));
    }

    ResultingSchema_ = New<TTableSchema>(std::move(resultingColumns));

    CanSkipLookup_ = CanSkipLookup_ && std::all_of(
        evaluatedExpressionsColumns.begin(),
        evaluatedExpressionsColumns.end(),
        [&] (const std::string& column) {
            return IsNotValueColumn(*tableSchema, column);
        });

    YT_LOG_DEBUG("Prepared secondary index modification pipeline (IntermediateSchema: %v, SkipLookup: %v, NameTable: %v)",
        ResultingSchema_,
        CanSkipLookup_,
        NameTable_->GetNames());
}

TFuture<void> TSecondaryIndexModifier::LookupRows()
{
    int keyColumnCount = TableMountInfo_->Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();

    std::vector<TUnversionedRow> lookupKeys;
    lookupKeys.reserve(MergedModifications_.size());
    for (const auto& modification : MergedModifications_) {
        auto key = TKey(modification.Row.FirstNElements(keyColumnCount));
        auto [_, inserted] = InitialRowMap_.insert({key, {}});
        if (inserted) {
            lookupKeys.push_back(RowBuffer_->CaptureRow(key.Elements()));
        }
    }

    if (CanSkipLookup_) {
        SetInitialAndResultingRows({});
        return OKFuture;
    }

    TLookupRowsOptions options;
    options.KeepMissingRows = false;
    auto nonEvaluatedColumns = TColumnFilter::TIndexes(
        PositionToIdMapping_.begin(),
        PositionToIdMapping_.begin() + FirstEvaluatedColumnPosition_);
    options.ColumnFilter = TColumnFilter(std::move(nonEvaluatedColumns));

    return Lookuper_(
        TableMountInfo_->Path,
        NameTable_,
        MakeSharedRange(std::move(lookupKeys), RowBuffer_),
        options)
        .AsUnique().Apply(BIND([&, this_ = MakeStrong(this)] (TSharedRange<TUnversionedRow>&& result) {
            SetInitialAndResultingRows(result);
        }));
}

void TSecondaryIndexModifier::InitializeMapping(std::vector<int> tableColumnIds)
{
    std::sort(tableColumnIds.begin(), tableColumnIds.end());
    tableColumnIds.erase(
        std::unique(tableColumnIds.begin(), tableColumnIds.end()),
        tableColumnIds.end());
    FirstEvaluatedColumnPosition_ = std::ssize(tableColumnIds);
    for (const auto& evaluatedColumn : EvaluatedColumnsSchema_) {
        tableColumnIds.push_back(NameTable_->GetIdOrRegisterName(evaluatedColumn.Name));
    }

    PositionToIdMapping_ = std::move(tableColumnIds);

    ResultingRowMapping_.assign(static_cast<size_t>(NameTable_->GetSize()), -1);
    for (int position = 0; position < std::ssize(PositionToIdMapping_); ++position) {
        ResultingRowMapping_[PositionToIdMapping_[position]] = position;
    }
}

void TSecondaryIndexModifier::SetInitialAndResultingRows(TSharedRange<TUnversionedRow> lookedUpRows)
{
    LookedUpRows_ = std::move(lookedUpRows);

    int keyColumnCount = TableMountInfo_->Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    int columnCount = std::ssize(PositionToIdMapping_);
    auto evaluator = ColumnEvaluatorCache_->Find(ResultingSchema_);

    for (auto initialRow : LookedUpRows_) {
        auto key = TKey(initialRow.FirstNElements(keyColumnCount));
        auto mutableRow = RowBuffer_->AllocateUnversioned(columnCount);

        YT_VERIFY(FirstEvaluatedColumnPosition_ == static_cast<int>(initialRow.GetCount()));
        for (int position = 0; position < FirstEvaluatedColumnPosition_; ++position) {
            mutableRow[position] = initialRow[position];
            mutableRow[position].Id = PositionToIdMapping_[position];
        }

        for (int position = FirstEvaluatedColumnPosition_; position < columnCount; ++position) {
            mutableRow[position].Id = PositionToIdMapping_[position];
            evaluator->EvaluateKey(mutableRow, RowBuffer_, position, /*preserveColumnId*/ true);
        }

        GetOrCrash(InitialRowMap_, key) = mutableRow;
    }

    for (const auto& [key, row] : InitialRowMap_) {
        EmplaceOrCrash(ResultingRowMap_, key, RowBuffer_->CaptureRow(row, /*captureValues*/ false));
    }

    for (const auto& modification : MergedModifications_) {
        auto key = TKey(modification.Row.FirstNElements(keyColumnCount));
        auto& alteredRow = GetOrCrash(ResultingRowMap_, key);

        switch (modification.Command) {
            case EWireProtocolCommand::WriteAndLockRow:
            case EWireProtocolCommand::WriteRow:
                if (!alteredRow) {
                    alteredRow = RowBuffer_->AllocateUnversioned(columnCount);
                    for (int index = 0; index < columnCount; ++index) {
                        alteredRow[index] = MakeUnversionedSentinelValue(
                            EValueType::Null,
                            PositionToIdMapping_[index]);
                    }
                }

                for (auto value : modification.Row) {
                    if (auto index = ResultingRowMapping_[value.Id]; index >= 0) {
                        alteredRow[index] = RowBuffer_->CaptureValue(value);
                    }
                }

                for (int position = FirstEvaluatedColumnPosition_; position < columnCount; ++position) {
                    evaluator->EvaluateKey(alteredRow, RowBuffer_, position, /*preserveColumnId*/ true);
                }

                break;

            case EWireProtocolCommand::DeleteRow:
                alteredRow = {};
                break;

            default:
                YT_ABORT();
        }
    }
}

TFuture<void> TSecondaryIndexModifier::OnIndexModifications(std::function<void(
    NYPath::TYPath path,
    TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications)> enqueueModificationRequests) const
{
    std::vector<TFuture<void>> modificationRequestEvents;
    modificationRequestEvents.reserve(IndexTableMountInfos_.size());

    for (int index = 0; index < std::ssize(IndexTableMountInfos_); ++index) {
        modificationRequestEvents.push_back(
            ProduceModificationsForIndex(index)
                .AsUnique().Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    index,
                    enqueueModificationRequests
                ] (TSharedRange<TRowModification>&& modifications) {
                    enqueueModificationRequests(
                        IndexTableMountInfos_[index]->Path,
                        NameTable_,
                        std::move(modifications));
                })));
    }

    return AllSucceeded(std::move(modificationRequestEvents));
}

TFuture<TSharedRange<TRowModification>> TSecondaryIndexModifier::ProduceModificationsForIndex(int index) const
{
    auto indexSchema = IndexTableMountInfos_[index]->Schemas[ETableSchemaKind::Write];
    auto indexIdMapping = BuildColumnIdMapping(
        *indexSchema,
        NameTable_,
        /*allowMissingKeyColumns*/ true);
    auto keyIndexIdMapping = BuildColumnIdMapping(
        *IndexTableMountInfos_[index]->Schemas[ETableSchemaKind::Lookup],
        NameTable_,
        /*allowMissingKeyColumns*/ true);

    std::optional<TUnversionedValue> emptyValue;
    if (indexSchema->FindColumn(EmptyValueColumnName)) {
        auto id = NameTable_->GetId(EmptyValueColumnName);
        emptyValue = MakeUnversionedNullValue(id);
    }

    switch (IndexDescriptors_[index].Kind) {
        case ESecondaryIndexKind::FullSync:
            return ProduceFullSyncModifications(
                std::move(indexIdMapping),
                std::move(keyIndexIdMapping),
                std::move(indexSchema),
                IndexDescriptors_[index].PredicatePosition,
                std::move(emptyValue));

        case ESecondaryIndexKind::Unfolding:
            return ProduceUnfoldingModifications(
                std::move(indexIdMapping),
                std::move(keyIndexIdMapping),
                std::move(indexSchema),
                IndexDescriptors_[index].PredicatePosition,
                std::move(emptyValue),
                *IndexDescriptors_[index].UnfoldedColumns,
                IndexDescriptors_[index].AggregateColumnPositions);

        case ESecondaryIndexKind::Unique:
            return ProduceUniqueModifications(
                IndexTableMountInfos_[index]->Path,
                std::move(indexIdMapping),
                std::move(keyIndexIdMapping),
                std::move(indexSchema),
                IndexDescriptors_[index].PredicatePosition,
                std::move(emptyValue));

        default:
            YT_ABORT();
    }
}

TFuture<TSharedRange<TRowModification>> TSecondaryIndexModifier::ProduceFullSyncModifications(
    TNameTableToSchemaIdMapping indexIdMapping,
    TNameTableToSchemaIdMapping keyIndexIdMapping,
    TTableSchemaPtr indexSchema,
    std::optional<int> predicatePosition,
    std::optional<TUnversionedValue> empty) const
{
    std::vector<TRowModification> secondaryModifications;

    for (const auto& [_, initialRow] : InitialRowMap_) {
        if (initialRow && IsPredicateGood(predicatePosition, initialRow)) {
            auto rowToDelete = RowBuffer_->CaptureAndPermuteRow(
                initialRow,
                *indexSchema,
                indexSchema->GetKeyColumnCount(),
                keyIndexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true);

            secondaryModifications.push_back(TRowModification{
                .Type = ERowModificationType::Delete,
                .Row = rowToDelete.ToTypeErasedRow(),
                .Locks = TLockMask(),
            });
        }
    }

    for (const auto& [_, resultingRow] : ResultingRowMap_) {
        if (resultingRow && IsPredicateGood(predicatePosition, resultingRow)) {
            auto rowToWrite = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                *indexSchema,
                indexSchema->GetKeyColumnCount(),
                indexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true,
                empty);

            secondaryModifications.push_back(TRowModification{
                .Type = ERowModificationType::Write,
                .Row = rowToWrite.ToTypeErasedRow(),
                .Locks = TLockMask(),
            });
        }
    }

    return MakeFuture(MakeSharedRange(std::move(secondaryModifications), RowBuffer_, LookedUpRows_));
}

TFuture<TSharedRange<TRowModification>> TSecondaryIndexModifier::ProduceUnfoldingModifications(
    TNameTableToSchemaIdMapping indexIdMapping,
    TNameTableToSchemaIdMapping keyIndexIdMapping,
    TTableSchemaPtr indexSchema,
    std::optional<int> predicatePosition,
    std::optional<TUnversionedValue> empty,
    const TUnfoldedColumns& unfoldedColumns,
    const std::vector<int>& aggregatePositions) const
{
    int unfoldedIndexColumnPosition = indexSchema->GetColumnIndexOrThrow(unfoldedColumns.IndexColumn);
    int unfoldedTableColumnPosition = ResultingSchema_->GetColumnIndexOrThrow(unfoldedColumns.TableColumn);
    int unfoldedIndexColumnId = NameTable_->GetIdOrThrow(unfoldedColumns.IndexColumn);

    std::vector<TRowModification> secondaryModifications;

    auto unfoldValue = [&] (TUnversionedRow row, std::function<void(TUnversionedRow)> consumeRow) {
        if (row[unfoldedIndexColumnPosition].Type == EValueType::Null) {
            return;
        }

        auto memoryInput = TMemoryInput(
            FromUnversionedValue<NYson::TYsonStringBuf>(row[unfoldedIndexColumnPosition])
            .AsStringBuf());

        auto parser = TYsonPullParser(&memoryInput, EYsonType::Node);
        auto cursor = TYsonPullParserCursor(&parser);

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto producedRow = RowBuffer_->CaptureRow(row, /*captureValues*/ false);
            auto& unfoldedValue = producedRow[unfoldedIndexColumnPosition];

            switch (auto type = cursor->GetCurrent().GetType()) {
                case EYsonItemType::EntityValue:
                    unfoldedValue.Type = EValueType::Null;
                    break;

                case EYsonItemType::Int64Value:
                    unfoldedValue.Type = EValueType::Int64;
                    unfoldedValue.Data.Int64 = cursor->GetCurrent().UncheckedAsInt64();
                    break;

                case EYsonItemType::Uint64Value:
                    unfoldedValue.Type = EValueType::Uint64;
                    unfoldedValue.Data.Uint64 = cursor->GetCurrent().UncheckedAsUint64();
                    break;

                case EYsonItemType::DoubleValue:
                    unfoldedValue.Type = EValueType::Double;
                    unfoldedValue.Data.Double = cursor->GetCurrent().UncheckedAsDouble();
                    break;

                case EYsonItemType::StringValue: {
                    auto value = cursor->GetCurrent().UncheckedAsString();
                    unfoldedValue.Type = EValueType::String;
                    unfoldedValue.Data.String = value.data();
                    unfoldedValue.Length = value.size();
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Unsupported type for unfolding index %Qlv", type);
            }

            consumeRow(producedRow);
            cursor->Next();
        });
    };

    for (auto [_, initialRow] : InitialRowMap_) {
        if (initialRow && IsPredicateGood(predicatePosition, initialRow)) {
            initialRow[unfoldedTableColumnPosition].Id = unfoldedIndexColumnId;
            auto permuttedRow = RowBuffer_->CaptureAndPermuteRow(
                initialRow,
                *indexSchema,
                indexSchema->GetKeyColumnCount(),
                keyIndexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true);

            unfoldValue(permuttedRow, [&] (TUnversionedRow rowToDelete) {
                secondaryModifications.push_back(TRowModification{
                    .Type = ERowModificationType::Delete,
                    .Row = rowToDelete.ToTypeErasedRow(),
                    .Locks = TLockMask(),
                });
            });
        }
    }

    for (auto [_, resultingRow] : ResultingRowMap_) {
        if (resultingRow && IsPredicateGood(predicatePosition, resultingRow)) {
            resultingRow[unfoldedTableColumnPosition].Id = unfoldedIndexColumnId;
            for (int position : aggregatePositions) {
                resultingRow[position].Flags |= EValueFlags::Aggregate;
            }
            auto permuttedRow = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                *indexSchema,
                indexSchema->GetKeyColumnCount(),
                indexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true,
                empty);

            for (int position : aggregatePositions) {
                resultingRow[position].Flags = EValueFlags::None;
            }

            unfoldValue(permuttedRow, [&] (TUnversionedRow rowToWrite) {
                secondaryModifications.push_back(TRowModification{
                    .Type = ERowModificationType::Write,
                    .Row = rowToWrite.ToTypeErasedRow(),
                    .Locks = TLockMask(),
                });
            });
        }
    }

    return MakeFuture(MakeSharedRange(std::move(secondaryModifications), RowBuffer_, LookedUpRows_));
}

TFuture<TSharedRange<TRowModification>> TSecondaryIndexModifier::ProduceUniqueModifications(
    const NYPath::TYPath& uniqueIndexPath,
    TNameTableToSchemaIdMapping indexIdMapping,
    TNameTableToSchemaIdMapping keyIndexIdMapping,
    TTableSchemaPtr indexSchema,
    std::optional<int> predicatePosition,
    std::optional<TUnversionedValue> empty) const
{
    return ValidateUniqueness(uniqueIndexPath, indexIdMapping, keyIndexIdMapping, *indexSchema, predicatePosition)
        .Apply(BIND(
            &TSecondaryIndexModifier::ProduceFullSyncModifications,
            MakeStrong(this),
            std::move(indexIdMapping),
            std::move(keyIndexIdMapping),
            std::move(indexSchema),
            std::move(predicatePosition),
            std::move(empty)));
}

TFuture<void> TSecondaryIndexModifier::ValidateUniqueness(
    const NYPath::TYPath& uniqueIndexPath,
    const TNameTableToSchemaIdMapping& indexIdMapping,
    const TNameTableToSchemaIdMapping& keyIndexIdMapping,
    const TTableSchema& indexWriteSchema,
    std::optional<int> predicatePosition) const
{
    TIndexKeyToTableKeyMap extraIndexKeys;
    for (const auto& [key, resultingRow] : ResultingRowMap_) {
        if (resultingRow && IsPredicateGood(predicatePosition, resultingRow)) {
            auto resultingIndexKey = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                indexWriteSchema,
                indexWriteSchema.GetKeyColumnCount(),
                keyIndexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true);

            auto [it, inserted] = extraIndexKeys.insert({resultingIndexKey, key});
            if (!inserted) {
                THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::UniqueIndexConflict,
                    "Conflict in unique index around key %v between writes to table by keys %v and %v",
                    it->first,
                    key,
                    it->second)
                    << TErrorAttribute("unique_index_path", uniqueIndexPath);
            }
        }
    }

    for (const auto& [key, initialRow] : InitialRowMap_) {
        if (initialRow && IsPredicateGood(predicatePosition, initialRow)) {
            auto initialIndexKey = RowBuffer_->CaptureAndPermuteRow(
                initialRow,
                indexWriteSchema,
                indexWriteSchema.GetKeyColumnCount(),
                keyIndexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true);

            extraIndexKeys.erase(initialIndexKey);
        }
    }

    if (extraIndexKeys.empty()) {
        return OKFuture;
    }

    auto keyTableIdMapping = BuildColumnIdMapping(
        *TableMountInfo_->Schemas[ETableSchemaKind::Primary]->ToKeys(),
        NameTable_);

    std::vector<TUnversionedRow> indexKeys;
    indexKeys.reserve(extraIndexKeys.size());
    for (const auto& [indexKey, _] : extraIndexKeys) {
        indexKeys.push_back(indexKey);
    }

    TColumnFilter::TIndexes tableKeyColumnIds;
    std::vector<int> tableKeyColumnPositions;
    for (int nameTableId = 0; nameTableId < NameTable_->GetSize(); ++nameTableId) {
        if (keyTableIdMapping[nameTableId] >= 0 && indexIdMapping[nameTableId] >= 0) {
            tableKeyColumnIds.push_back(nameTableId);
            tableKeyColumnPositions.push_back(keyTableIdMapping[nameTableId]);
        }
    }

    TLookupRowsOptions options;
    options.ColumnFilter = TColumnFilter(std::move(tableKeyColumnIds));
    options.KeepMissingRows = true;
    return Lookuper_(
        uniqueIndexPath,
        NameTable_,
        MakeSharedRange(indexKeys),
        options)
        .AsUnique().Apply(BIND([
            uniqueIndexPath,
            extraIndexKeys = std::move(extraIndexKeys),
            indexKeys = std::move(indexKeys),
            tableKeyColumnPositions = std::move(tableKeyColumnPositions)
        ] (TSharedRange<TUnversionedRow>&& indexRows) {
            for (int rowNumber = 0; rowNumber < std::ssize(indexKeys); ++rowNumber) {
                auto indexRow = indexRows[rowNumber];
                if (!indexRow) {
                    continue;
                }

                auto indexKey = indexKeys[rowNumber];
                auto tableKey = GetOrCrash(extraIndexKeys, indexKey);

                for (int position = 0; position < std::ssize(tableKeyColumnPositions); ++position) {
                    auto tableKeyColumnInIndex = indexRow[position];
                    auto keyColumnPosition = tableKeyColumnPositions[position];

                    YT_VERIFY(keyColumnPosition >= 0 && keyColumnPosition < tableKey.GetLength());

                    if (tableKey[keyColumnPosition] != tableKeyColumnInIndex) {
                        THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::UniqueIndexConflict,
                            "Conflict in unique index around index key %v. Write with the key %v "
                            "conflicts with the row %v present in the index table",
                            indexKey,
                            tableKey,
                            indexRow)
                            << TErrorAttribute("unique_index_path", uniqueIndexPath);
                    }
                }
            }
        }));
}

bool TSecondaryIndexModifier::IsPredicateGood(
    std::optional<int> predicatePosition,
    TUnversionedRow row) const
{
    if (!predicatePosition) {
        return true;
    }

    const auto& value = row[FirstEvaluatedColumnPosition_ + *predicatePosition];
    if (value.Type == EValueType::Null) {
        return false;
    }

    YT_VERIFY(value.Type == EValueType::Boolean);

    return value.Data.Boolean;
}

bool TSecondaryIndexModifier::IsNotValueColumn(const TTableSchema& schema, TStringBuf columnName)
{
    const auto* column = schema.FindColumn(columnName);
    return !column || column->SortOrder();
}

bool TSecondaryIndexModifier::AreIndexColumnsDependentOnlyOnKeyColumns(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const std::optional<TUnfoldedColumns>& unfoldedColumns)
{
    for (const auto& indexKeyColumn : indexTableSchema.Columns()) {
        if (!indexKeyColumn.SortOrder()) {
            break;
        }

        auto tableColumnName = TStringBuf(indexKeyColumn.Name());
        if (unfoldedColumns && unfoldedColumns->IndexColumn == tableColumnName) {
            tableColumnName = unfoldedColumns->TableColumn;
        }

        if (const auto* tableColumn = tableSchema.FindColumn(tableColumnName)) {
            if (!tableColumn->SortOrder()) {
                return false;
            } else {
                continue;
            }
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

std::function<TLookupSignature> MakeLookuper(ITransactionPtr transaction)
{
    return [transaction = std::move(transaction)] (
        const NYPath::TYPath& path,
        TNameTablePtr nameTable,
        TSharedRange<TLegacyKey> keys,
        const TLookupRowsOptions& options)
    {
        return transaction->LookupRows(path, std::move(nameTable), keys, options)
            .AsUnique().Apply(BIND([] (TUnversionedLookupRowsResult&& result) {
                return result.Rowset->GetRows();
            }));
    };
}

////////////////////////////////////////////////////////////////////////////////

ISecondaryIndexModifierPtr CreateSecondaryIndexModifier(
    ITransactionPtr transaction,
    NTabletClient::TTableMountInfoPtr tableMountInfo,
    std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
    TRange<TUnversionedSubmittedRow> mergedModifications,
    NQueryClient::IColumnEvaluatorCachePtr columnEvaluatorCache,
    NLogging::TLogger logger)
{
    return New<TSecondaryIndexModifier>(
        MakeLookuper(std::move(transaction)),
        std::move(tableMountInfo),
        std::move(indexMountInfos),
        std::move(mergedModifications),
        std::move(columnEvaluatorCache),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

ISecondaryIndexModifierPtr CreateSecondaryIndexModifier(
    std::function<TLookupSignature> lookuper,
    NTabletClient::TTableMountInfoPtr tableMountInfo,
    std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
    TRange<TUnversionedSubmittedRow> mergedModifications)
{
    return New<TSecondaryIndexModifier>(
        std::move(lookuper),
        std::move(tableMountInfo),
        std::move(indexMountInfos),
        std::move(mergedModifications),
        CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>()),
        TLogger("UnitTest"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
