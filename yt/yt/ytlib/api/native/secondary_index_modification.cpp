#include "tablet_helpers.h"
#include "secondary_index_modification.h"
#include "transaction.h"

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/engine_api/expression_evaluator.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/key.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NApi::NNative {

using namespace NLogging;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TSecondaryIndexModificationsBufferTag { };

using NAst::TReferenceHarvester;

////////////////////////////////////////////////////////////////////////////////

TSecondaryIndexModifier::TSecondaryIndexModifier(
    TTableMountInfoPtr tableMountInfo,
    std::vector<TTableMountInfoPtr> indexInfos,
    TRange<TUnversionedSubmittedRow> mergedModifications,
    IExpressionEvaluatorCachePtr expressionEvaluatorCache,
    TLogger logger)
    : TableMountInfo_(std::move(tableMountInfo))
    , IndexInfos_(std::move(indexInfos))
    , NameTable_(TNameTable::FromSchema(*TableMountInfo_->Schemas[ETableSchemaKind::Primary]))
    , MergedModifications_(std::move(mergedModifications))
    , ExpressionEvaluatorCache_(std::move(expressionEvaluatorCache))
    , RowBuffer_(New<TRowBuffer>(TSecondaryIndexModificationsBufferTag{}))
    , Logger(std::move(logger))
{
    YT_VERIFY(TableMountInfo_->Indices.size() == IndexInfos_.size());

    YT_LOG_DEBUG("Building secondary index modifications (TablePath: %v, Indices: [%v])",
        TableMountInfo_->Path,
        MakeFormattableView(
            TableMountInfo_->Indices,
            [] (auto* builder, const auto& indexInfo) {
                builder->AppendFormat("(%v: %Qlv)", indexInfo.TableId, indexInfo.Kind);
            }));

    for (const auto& keyColumn : TableMountInfo_->Schemas[ETableSchemaKind::Primary]->Columns()) {
        if (!keyColumn.SortOrder()) {
            break;
        }
        PositionToIdMapping_.push_back(NameTable_->GetId(keyColumn.Name()));
    }

    TColumnSet predicateColumns;
    IndexDescriptors_.resize(IndexInfos_.size());
    for (int index = 0; index < std::ssize(IndexInfos_); ++index) {
        auto& descriptor = IndexDescriptors_[index];
        const auto& indexInfo = IndexInfos_[index];
        const auto& indexSchema = *indexInfo->Schemas[ETableSchemaKind::Write];

        if (const auto& predicate = TableMountInfo_->Indices[index].Predicate) {
            auto parsedSource = ParseSource(*predicate, EParseMode::Expression);
            TReferenceHarvester(&predicateColumns)
                .Visit(std::get<NAst::TExpressionPtr>(parsedSource->AstHead.Ast));
            descriptor.Predicate = std::move(parsedSource);
        }

        switch (auto kind = descriptor.Kind = TableMountInfo_->Indices[index].Kind) {
            case ESecondaryIndexKind::FullSync:
                ValidateFullSyncIndexSchema(
                    *TableMountInfo_->Schemas[ETableSchemaKind::Write],
                    indexSchema);
                break;

            case ESecondaryIndexKind::Unfolding:
                descriptor.UnfoldedColumnPosition = indexSchema.GetColumnIndex(
                    FindUnfoldingColumnAndValidate(
                        *TableMountInfo_->Schemas[ETableSchemaKind::Write],
                        indexSchema));
                break;

            default:
                THROW_ERROR_EXCEPTION("Unsupported secondary index kind %Qlv",
                    kind);
        }

        for (const auto& column : indexSchema.Columns()) {
            auto id = NameTable_->GetIdOrRegisterName(column.Name());
            if (TableMountInfo_->Schemas[ETableSchemaKind::Primary]->FindColumn(column.Name())) {
                PositionToIdMapping_.push_back(id);
            }
        }
    }

    for (const auto& column : predicateColumns) {
        PositionToIdMapping_.push_back(NameTable_->GetId(column));
    }

    std::sort(PositionToIdMapping_.begin(), PositionToIdMapping_.end());
    PositionToIdMapping_.erase(
        std::unique(PositionToIdMapping_.begin(), PositionToIdMapping_.end()),
        PositionToIdMapping_.end());

    ResultingRowMapping_.assign(static_cast<size_t>(NameTable_->GetSize()), -1);
    for (int position = 0; position < std::ssize(PositionToIdMapping_); ++position) {
        ResultingRowMapping_[PositionToIdMapping_[position]] = position;
    }

    std::vector<TColumnSchema> resultingColumns;
    resultingColumns.reserve(PositionToIdMapping_.size());
    for (int id : PositionToIdMapping_) {
        resultingColumns.push_back(TableMountInfo_
            ->Schemas[ETableSchemaKind::Primary]
            ->GetColumn(NameTable_->GetName(id)));
    }
    ResultingSchema_ = New<TTableSchema>(std::move(resultingColumns));
}

TFuture<void> TSecondaryIndexModifier::LookupRows(ITransaction* transaction)
{
    std::vector<TUnversionedRow> lookupKeys;
    lookupKeys.reserve(MergedModifications_.size());
    for (const auto& modification : MergedModifications_) {
        auto key = TKey(modification.Row.FirstNElements(TableMountInfo_
            ->Schemas[ETableSchemaKind::Primary]
            ->GetKeyColumnCount()));
        auto [_, inserted] = InitialRowMap_.insert({key, {}});
        if (inserted) {
            lookupKeys.push_back(RowBuffer_->CaptureRow(key.Elements()));
        }
    }

    TLookupRowsOptions options;
    options.KeepMissingRows = false;
    options.ColumnFilter = TColumnFilter(PositionToIdMapping_);

    return transaction->LookupRows(
        TableMountInfo_->Path,
        NameTable_,
        MakeSharedRange(std::move(lookupKeys), RowBuffer_),
        options)
        .Apply(BIND([&] (const TUnversionedLookupRowsResult& result) {
            SetInitialAndResultingRows(result.Rowset->GetRows());
        }));
}

void TSecondaryIndexModifier::SetInitialAndResultingRows(TSharedRange<NTableClient::TUnversionedRow> lookedUpRows)
{
    int keyColumnCount = TableMountInfo_->Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    for (auto initialRow : lookedUpRows) {
        auto key = TKey(initialRow.FirstNElements(keyColumnCount));
        auto mutableRow = RowBuffer_->CaptureRow(initialRow);

        YT_VERIFY(PositionToIdMapping_.size() == mutableRow.GetCount());
        for (int index = 0; index < std::ssize(PositionToIdMapping_); ++index) {
            mutableRow[index].Id = PositionToIdMapping_[index];
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
                    alteredRow = RowBuffer_->AllocateUnversioned(PositionToIdMapping_.size());
                    for (int index = 0; index < std::ssize(PositionToIdMapping_); ++index) {
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

                break;

            case EWireProtocolCommand::DeleteRow:
                alteredRow = {};
                break;

            default:
                YT_ABORT();
        }
    }
}

void TSecondaryIndexModifier::OnIndexModifications(std::function<void(
    NYPath::TYPath path,
    NTableClient::TNameTablePtr nameTable,
    TSharedRange<TRowModification> modifications)> enqueueModificationRequests)
{
    for (int index = 0; index < std::ssize(IndexInfos_); ++index) {
        const auto& indexInfo = IndexInfos_[index];
        auto modifications = ProduceModificationsForIndex(index);

        enqueueModificationRequests(
            indexInfo->Path,
            NameTable_,
            std::move(modifications));
    }
}

TSharedRange<TRowModification> TSecondaryIndexModifier::ProduceModificationsForIndex(int index)
{
    const auto& indexSchema = *IndexInfos_[index]->Schemas[ETableSchemaKind::Write];
    auto indexIdMapping = BuildColumnIdMapping(
        indexSchema,
        NameTable_,
        /*allowMissingKeyColumns*/ true);
    auto keyIndexIdMapping = BuildColumnIdMapping(
        *IndexInfos_[index]->Schemas[ETableSchemaKind::Lookup],
        NameTable_,
        /*allowMissingKeyColumns*/ true);

    std::function<bool(TUnversionedRow)> predicate;
    if (const auto& parsedPredicate = IndexDescriptors_[index].Predicate) {
        auto evaluator = ExpressionEvaluatorCache_->Find(*parsedPredicate, ResultingSchema_);
        predicate = [evaluator = std::move(evaluator), rowBuffer = RowBuffer_] (const TUnversionedRow row) {
            auto result = evaluator->Evaluate(row, rowBuffer);

            switch (result.Type) {
                case EValueType::Boolean:
                    return result.Data.Boolean;

                case EValueType::Null:
                    return false;

                default:
                    THROW_ERROR_EXCEPTION("Predicate computed to an unexpected type %Qlv", result.Type);
            }
        };
    } else {
        predicate = [] (const TUnversionedRow) {
            return true;
        };
    }

    std::optional<TUnversionedValue> emptyValue;
    if (indexSchema.FindColumn(EmptyValueColumnName)) {
        auto id = NameTable_->GetId(EmptyValueColumnName);
        emptyValue = MakeUnversionedNullValue(id);
    }

    switch (IndexDescriptors_[index].Kind) {
        case ESecondaryIndexKind::FullSync:
            return ProduceFullSyncModifications(
                indexIdMapping,
                keyIndexIdMapping,
                indexSchema,
                predicate,
                emptyValue);

        case ESecondaryIndexKind::Unfolding:
            return ProduceUnfoldingModifications(
                indexIdMapping,
                keyIndexIdMapping,
                indexSchema,
                predicate,
                emptyValue,
                *IndexDescriptors_[index].UnfoldedColumnPosition);

        default:
            YT_ABORT();
    }
}

TSharedRange<TRowModification> TSecondaryIndexModifier::ProduceFullSyncModifications(
    const TNameTableToSchemaIdMapping& indexIdMapping,
    const TNameTableToSchemaIdMapping& keyIndexIdMapping,
    const TTableSchema& indexSchema,
    std::function<bool(TUnversionedRow)> predicate,
    const std::optional<TUnversionedValue>& empty) const
{
    std::vector<TRowModification> secondaryModifications;

    for (const auto& [_, initialRow] : InitialRowMap_) {
        if (initialRow && predicate(initialRow)) {
            auto rowToDelete = RowBuffer_->CaptureAndPermuteRow(
                initialRow,
                indexSchema,
                indexSchema.GetKeyColumnCount(),
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
        if (resultingRow && predicate(resultingRow)) {
            auto rowToWrite = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                indexSchema,
                indexSchema.GetKeyColumnCount(),
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

    return MakeSharedRange(std::move(secondaryModifications), RowBuffer_);
}

TSharedRange<TRowModification> TSecondaryIndexModifier::ProduceUnfoldingModifications(
    const TNameTableToSchemaIdMapping& indexIdMapping,
    const TNameTableToSchemaIdMapping& keyIndexIdMapping,
    const TTableSchema& indexSchema,
    std::function<bool(TUnversionedRow)> predicate,
    const std::optional<TUnversionedValue>& empty,
    int unfoldedKeyPosition) const
{
    std::vector<TRowModification> secondaryModifications;

    auto unfoldValue = [&] (TUnversionedRow row, std::function<void(TUnversionedRow)> consumeRow) {
        if (row[unfoldedKeyPosition].Type == EValueType::Null) {
            return;
        }

        auto memoryInput = TMemoryInput(
            FromUnversionedValue<NYson::TYsonStringBuf>(row[unfoldedKeyPosition])
            .AsStringBuf());

        auto parser = TYsonPullParser(&memoryInput, EYsonType::Node);
        auto cursor = TYsonPullParserCursor(&parser);

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto producedRow = RowBuffer_->CaptureRow(row, /*captureValues*/ false);
            auto& unfoldedValue = producedRow[unfoldedKeyPosition];

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
        if (initialRow && predicate(initialRow)) {
            auto permuttedRow = RowBuffer_->CaptureAndPermuteRow(
                initialRow,
                indexSchema,
                indexSchema.GetKeyColumnCount(),
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
        if (resultingRow && predicate(resultingRow)) {
            auto permuttedRow = RowBuffer_->CaptureAndPermuteRow(
                resultingRow,
                indexSchema,
                indexSchema.GetKeyColumnCount(),
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

    return MakeSharedRange(std::move(secondaryModifications), RowBuffer_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
