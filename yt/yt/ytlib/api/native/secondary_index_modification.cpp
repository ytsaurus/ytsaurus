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
    std::optional<int> UnfoldedColumnPosition;
    std::optional<int> PredicatePosition;
};

struct TEvaluatedColumn
{
    std::string Name;
    TString Expression;
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
        std::vector<NTabletClient::TTableMountInfoPtr> indexMountInfos,
        TRange<TUnversionedSubmittedRow> mergedModifications,
        NQueryClient::IColumnEvaluatorCachePtr columnEvaluatorCache,
        NLogging::TLogger logger);

    TFuture<void> LookupRows() override;

    TFuture<void> OnIndexModifications(std::function<void(
        NYPath::TYPath path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<TRowModification> modifications)> enqueueModificationRequests) const override;

private:
    using TInitialRowMap = THashMap<NTableClient::TKey, NTableClient::TUnversionedRow>;
    using TResultingRowMap = THashMap<NTableClient::TKey, NTableClient::TMutableUnversionedRow>;
    using TIndexKeyToTableKeyMap = THashMap<NTableClient::TUnversionedRow, NTableClient::TKey>;

    const std::function<TLookupSignature> Lookuper_;
    const NTabletClient::TTableMountInfoPtr TableMountInfo_;
    const std::vector<NTabletClient::TTableMountInfoPtr> IndexTableMountInfos_;
    const NTableClient::TNameTablePtr NameTable_;
    const TRange<TUnversionedSubmittedRow> MergedModifications_;
    const NQueryClient::IColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const NTableClient::TRowBufferPtr RowBuffer_;

    const NLogging::TLogger Logger;

    std::vector<TIndexDescriptor> IndexDescriptors_;
    std::vector<int> UnfoldedColumnIndices_;

    NTableClient::TNameTableToSchemaIdMapping ResultingRowMapping_;
    std::vector<int> PositionToIdMapping_;
    int FirstEvaluatedColumnPosition_;
    std::vector<TEvaluatedColumn> EvaluatedColumnsSchema_;
    NTableClient::TTableSchemaPtr ResultingSchema_;

    TInitialRowMap InitialRowMap_;
    TResultingRowMap ResultingRowMap_;

    void SetInitialAndResultingRows(TSharedRange<NTableClient::TUnversionedRow> lookedUpRows);

    TFuture<TSharedRange<TRowModification>> ProduceModificationsForIndex(int index) const;

    TFuture<TSharedRange<TRowModification>> ProduceFullSyncModifications(
        NTableClient::TNameTableToSchemaIdMapping indexIdMapping,
        NTableClient::TNameTableToSchemaIdMapping keyIndexIdMapping,
        NTableClient::TTableSchemaPtr indexSchema,
        std::optional<int> predicatePosition,
        std::optional<NTableClient::TUnversionedValue> empty) const;

    TFuture<TSharedRange<TRowModification>> ProduceUnfoldingModifications(
        NTableClient::TNameTableToSchemaIdMapping indexIdMapping,
        NTableClient::TNameTableToSchemaIdMapping keyIndexIdMapping,
        NTableClient::TTableSchemaPtr indexSchema,
        std::optional<int> predicatePosition,
        std::optional<NTableClient::TUnversionedValue> empty,
        int unfoldedKeyPosition) const;

    TFuture<TSharedRange<TRowModification>> ProduceUniqueModifications(
        const NYPath::TYPath& uniqueIndexPath,
        NTableClient::TNameTableToSchemaIdMapping indexIdMapping,
        NTableClient::TNameTableToSchemaIdMapping keyIndexIdMapping,
        NTableClient::TTableSchemaPtr indexSchema,
        std::optional<int> predicatePosition,
        std::optional<NTableClient::TUnversionedValue> empty) const;

    TFuture<void> ValidateUniqueness(
        const NYPath::TYPath& uniqueIndexPath,
        const NTableClient::TNameTableToSchemaIdMapping& indexIdMapping,
        const NTableClient::TNameTableToSchemaIdMapping& keyIndexIdMapping,
        const NTableClient::TTableSchema& indexWriteSchema,
        std::optional<int> predicatePosition) const;

    bool IsPredicateGood(std::optional<int> predicatePosition, TUnversionedRow value) const;
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

    for (const auto& keyColumn : tableSchema->Columns()) {
        if (!keyColumn.SortOrder()) {
            break;
        }
        PositionToIdMapping_.push_back(NameTable_->GetId(keyColumn.Name()));
    }

    TColumnSet evaluatedExpressionsColumns;
    IndexDescriptors_.resize(IndexTableMountInfos_.size());
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
            for (auto& column : indexMeta.EvaluatedColumnsSchema->Columns()) {
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

        // COMPAT(sabdenovch)
        if (descriptor.Kind == ESecondaryIndexKind::Unfolding && !indexMeta.UnfoldedColumn) {
            descriptor.UnfoldedColumnPosition = indexSchema.GetColumnIndex(
                FindUnfoldingColumnAndValidate(
                    *tableSchema,
                    indexSchema,
                    indexMeta.Predicate,
                    indexMeta.EvaluatedColumnsSchema));
        } else {
            ValidateIndexSchema(
                descriptor.Kind,
                *tableSchema,
                indexSchema,
                indexMeta.Predicate,
                indexMeta.EvaluatedColumnsSchema,
                indexMeta.UnfoldedColumn);

            if (indexMeta.UnfoldedColumn) {
                descriptor.UnfoldedColumnPosition = indexSchema.GetColumnIndex(*indexMeta.UnfoldedColumn);
            }
        }

        for (const auto& column : indexSchema.Columns()) {
            auto id = NameTable_->GetIdOrRegisterName(column.Name());
            if (TableMountInfo_->Schemas[ETableSchemaKind::Primary]->FindColumn(column.Name())) {
                PositionToIdMapping_.push_back(id);
            }
        }
    }

    for (const auto& column : evaluatedExpressionsColumns) {
        PositionToIdMapping_.push_back(NameTable_->GetId(column));
    }
    std::sort(PositionToIdMapping_.begin(), PositionToIdMapping_.end());
    PositionToIdMapping_.erase(
        std::unique(PositionToIdMapping_.begin(), PositionToIdMapping_.end()),
        PositionToIdMapping_.end());
    FirstEvaluatedColumnPosition_ = std::ssize(PositionToIdMapping_);
    for (const auto& evaluatedColumn : EvaluatedColumnsSchema_) {
        PositionToIdMapping_.push_back(NameTable_->GetIdOrRegisterName(evaluatedColumn.Name));
    }

    ResultingRowMapping_.assign(static_cast<size_t>(NameTable_->GetSize()), -1);
    for (int position = 0; position < std::ssize(PositionToIdMapping_); ++position) {
        ResultingRowMapping_[PositionToIdMapping_[position]] = position;
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

    TLookupRowsOptions options;
    options.KeepMissingRows = false;
    auto nonEvaluatedColumns = std::vector(
        PositionToIdMapping_.begin(),
        PositionToIdMapping_.begin() + FirstEvaluatedColumnPosition_);
    options.ColumnFilter = TColumnFilter(std::move(nonEvaluatedColumns));

    return Lookuper_(
        TableMountInfo_->Path,
        NameTable_,
        MakeSharedRange(std::move(lookupKeys), RowBuffer_),
        options)
        .ApplyUnique(BIND([&, this_ = MakeStrong(this)] (TSharedRange<TUnversionedRow>&& result) {
            SetInitialAndResultingRows(result);
        }));
}

void TSecondaryIndexModifier::SetInitialAndResultingRows(TSharedRange<NTableClient::TUnversionedRow> lookedUpRows)
{
    int keyColumnCount = TableMountInfo_->Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    int columnCount = std::ssize(PositionToIdMapping_);
    auto evaluator = ColumnEvaluatorCache_->Find(ResultingSchema_);

    for (auto initialRow : lookedUpRows) {
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
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications)> enqueueModificationRequests) const
{
    std::vector<TFuture<void>> modificationRequestEvents;

    for (int index = 0; index < std::ssize(IndexTableMountInfos_); ++index) {
        modificationRequestEvents.push_back(
            ProduceModificationsForIndex(index)
                .ApplyUnique(BIND([
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
                *IndexDescriptors_[index].UnfoldedColumnPosition);

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
                ERowModificationType::Delete,
                rowToDelete.ToTypeErasedRow(),
                TLockMask(),
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
                ERowModificationType::Write,
                rowToWrite.ToTypeErasedRow(),
                TLockMask(),
            });
        }
    }

    return MakeFuture(MakeSharedRange(std::move(secondaryModifications), RowBuffer_));
}

TFuture<TSharedRange<TRowModification>> TSecondaryIndexModifier::ProduceUnfoldingModifications(
    TNameTableToSchemaIdMapping indexIdMapping,
    TNameTableToSchemaIdMapping keyIndexIdMapping,
    TTableSchemaPtr indexSchema,
    std::optional<int> predicatePosition,
    std::optional<TUnversionedValue> empty,
    int unfoldedColumnPosition) const
{
    std::vector<TRowModification> secondaryModifications;

    auto unfoldValue = [&] (TUnversionedRow row, std::function<void(TUnversionedRow)> consumeRow) {
        if (row[unfoldedColumnPosition].Type == EValueType::Null) {
            return;
        }

        auto memoryInput = TMemoryInput(
            FromUnversionedValue<NYson::TYsonStringBuf>(row[unfoldedColumnPosition])
            .AsStringBuf());

        auto parser = TYsonPullParser(&memoryInput, EYsonType::Node);
        auto cursor = TYsonPullParserCursor(&parser);

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto producedRow = RowBuffer_->CaptureRow(row, /*captureValues*/ false);
            auto& unfoldedValue = producedRow[unfoldedColumnPosition];

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

    for (const auto& [_, initialRow] : InitialRowMap_) {
        if (initialRow && IsPredicateGood(predicatePosition, initialRow)) {
            auto permuttedRow = RowBuffer_->CaptureAndPermuteRow(
                initialRow,
                *indexSchema,
                indexSchema->GetKeyColumnCount(),
                keyIndexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true);

            unfoldValue(permuttedRow, [&] (TUnversionedRow rowToDelete) {
                secondaryModifications.push_back(TRowModification{
                    ERowModificationType::Delete,
                    rowToDelete.ToTypeErasedRow(),
                    TLockMask(),
                });
            });
        }
    }

    for (const auto& [_, resultingRow] : ResultingRowMap_) {
        if (resultingRow && IsPredicateGood(predicatePosition, resultingRow)) {
            auto permuttedRow = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                *indexSchema,
                indexSchema->GetKeyColumnCount(),
                indexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true,
                empty);

            unfoldValue(permuttedRow, [&] (TUnversionedRow rowToWrite) {
                secondaryModifications.push_back(TRowModification{
                    ERowModificationType::Write,
                    rowToWrite.ToTypeErasedRow(),
                    TLockMask(),
                });
            });
        }
    }

    return MakeFuture(MakeSharedRange(std::move(secondaryModifications), RowBuffer_));
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
    const TTableSchema& indexSchema,
    std::optional<int> predicatePosition) const
{
    TIndexKeyToTableKeyMap extraIndexKeys;
    for (const auto& [key, resultingRow] : ResultingRowMap_) {
        if (resultingRow && IsPredicateGood(predicatePosition, resultingRow)) {
            auto resultingIndexKey = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                indexSchema,
                indexSchema.GetKeyColumnCount(),
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
                indexSchema,
                indexSchema.GetKeyColumnCount(),
                keyIndexIdMapping,
                /*validateDuplicateAndRequiredValueColumns*/ false,
                /*preserveIds*/ true);

            extraIndexKeys.erase(initialIndexKey);
        }
    }

    if (extraIndexKeys.empty()) {
        return VoidFuture;
    }

    auto keyTableIdMapping = BuildColumnIdMapping(
        *TableMountInfo_->Schemas[ETableSchemaKind::Primary]->ToKeys(),
        NameTable_);

    std::vector<TUnversionedRow> indexKeys;
    indexKeys.reserve(extraIndexKeys.size());
    for (const auto& [indexKey, _] : extraIndexKeys) {
        indexKeys.push_back(indexKey);
    }

    std::vector<int> tableKeyColumnIds;
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
        .ApplyUnique(BIND([
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

////////////////////////////////////////////////////////////////////////////////

std::function<TLookupSignature> MakeLookuper(ITransactionPtr transaction)
{
    return [transaction = std::move(transaction)] (
        NYPath::TYPath path,
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TLegacyKey> keys,
        TLookupRowsOptions options)
    {
        return transaction->LookupRows(path, nameTable, keys, options)
            .ApplyUnique(BIND([] (TUnversionedLookupRowsResult&& result) {
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
        MakeLookuper(transaction),
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
