#include "skiff_writer.h"
#include "config.h"

#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/finally.h>

#include <yt/core/skiff/skiff.h>
#include <yt/core/skiff/skiff_schema.h>

#include <yt/core/skiff/schema_match.h>

#include <yt/core/yson/writer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

using NYTree::ConvertTo;

using namespace NSkiff;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MissingSystemColumn = -1;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESkiffWriterColumnType,
    (Unknown)
    (Dense)
    (Sparse)
    (Skip)
    (RangeIndex)
    (RowIndex)
);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ResizeToContainIndex(std::vector<T>* vec, size_t index)
{
    if (vec->size() < index + 1) {
        vec->resize(index + 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TSkiffEncodingInfo
{
    ESkiffWriterColumnType EncodingPart = ESkiffWriterColumnType::Unknown;

    EWireType WireType = EWireType::Nothing;
    TSkiffSchemaPtr SkiffType = nullptr;
    ui32 FieldIndex = 0; // index inside sparse / dense part of the row
    bool Required = false;

    TSkiffEncodingInfo() = default;

    static TSkiffEncodingInfo Skip()
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Skip;
        return result;
    }

    static TSkiffEncodingInfo RangeIndex(ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::RangeIndex;
        result.FieldIndex = fieldIndex;
        return result;
    }

    static TSkiffEncodingInfo RowIndex(ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::RowIndex;
        result.FieldIndex = fieldIndex;
        return result;
    }

    static TSkiffEncodingInfo Dense(TSkiffSchemaPtr schema, bool required, ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Dense;
        result.WireType = schema->GetWireType();
        result.SkiffType = schema;
        result.FieldIndex = fieldIndex;
        result.Required = required;
        return result;
    }

    static TSkiffEncodingInfo Sparse(TSkiffSchemaPtr schema, ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Sparse;
        result.WireType = schema->GetWireType();
        result.SkiffType = schema;
        result.FieldIndex = fieldIndex;
        result.Required = true;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSparseFieldInfo
{
    NSkiff::EWireType WireType;
    ui32 SparseFieldIndex;
    ui32 ValueIndex;

    TSparseFieldInfo(EWireType wireType, ui32 sparseFieldIndex, ui32 valueIndex)
        : WireType(wireType)
        , SparseFieldIndex(sparseFieldIndex)
        , ValueIndex(valueIndex)
    { }
};

struct TDenseFieldWriterInfo
{
    NSkiff::EWireType WireType;
    ui16 ColumnId;
    bool Required = false;

    TDenseFieldWriterInfo(NSkiff::EWireType type, ui16 columnId, bool required)
        : WireType(type)
        , ColumnId(columnId)
        , Required(required)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TSkiffWriterTableDescription
{
    std::vector<TSkiffEncodingInfo> KnownFields;
    std::vector<TDenseFieldWriterInfo> DenseFieldInfos;
    bool HasOtherColumns = false;
    int KeySwitchFieldIndex = -1;
    int RangeIndexFieldIndex = -1;
    int RowIndexFieldIndex = -1;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffSchemalessWriter
    : public TSchemalessFormatWriterBase
{
public:
    TSkiffSchemalessWriter(
        TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount)
        : TSchemalessFormatWriterBase(
            std::move(nameTable),
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount)
    { }

    void Init(std::vector<TSkiffSchemaPtr> tableSkiffSchemas)
    {
        auto streamSchema = CreateVariant16Schema(tableSkiffSchemas);
        SkiffWriter_.emplace(streamSchema, GetOutputStream());

        auto tableDescriptionList = CreateTableDescriptionList(tableSkiffSchemas, RangeIndexColumnName, RowIndexColumnName);
        for (const auto& commonTableDescription : tableDescriptionList) {
            TableDescriptionList_.emplace_back();
            auto& writerTableDescription = TableDescriptionList_.back();
            writerTableDescription.HasOtherColumns = commonTableDescription.HasOtherColumns;
            writerTableDescription.KeySwitchFieldIndex = commonTableDescription.KeySwitchFieldIndex.value_or(MissingSystemColumn);
            writerTableDescription.RowIndexFieldIndex = commonTableDescription.RowIndexFieldIndex.value_or(MissingSystemColumn);
            writerTableDescription.RangeIndexFieldIndex = commonTableDescription.RangeIndexFieldIndex.value_or(MissingSystemColumn);

            auto& knownFields = writerTableDescription.KnownFields;

            const auto& denseFieldDescriptionList = commonTableDescription.DenseFieldDescriptionList;

            auto& denseFieldWriterInfos = writerTableDescription.DenseFieldInfos;

            for (size_t i = 0; i < denseFieldDescriptionList.size(); ++i) {
                const auto& denseField = denseFieldDescriptionList[i];
                const auto id = NameTable_->GetIdOrRegisterName(denseField.Name);
                ResizeToContainIndex(&knownFields, id);
                YCHECK(knownFields[id].EncodingPart == ESkiffWriterColumnType::Unknown);
                knownFields[id] = TSkiffEncodingInfo::Dense(
                    denseField.DeoptionalizedSchema,
                    denseField.Required,
                    i);

                denseFieldWriterInfos.emplace_back(
                    denseField.DeoptionalizedSchema->GetWireType(),
                    id,
                    denseField.Required);
            }

            const auto& sparseFieldDescriptionList = commonTableDescription.SparseFieldDescriptionList;
            for (size_t i = 0; i < sparseFieldDescriptionList.size(); ++i) {
                const auto& sparseField = sparseFieldDescriptionList[i];
                auto id = NameTable_->GetIdOrRegisterName(sparseField.Name);
                ResizeToContainIndex(&knownFields, id);
                YCHECK(knownFields[id].EncodingPart == ESkiffWriterColumnType::Unknown);
                knownFields[id] = TSkiffEncodingInfo::Sparse(
                    sparseField.DeoptionalizedSchema,
                    i);
            }

            const auto systemColumnMaxId = Max(Max(GetTableIndexColumnId(), GetRangeIndexColumnId()), GetRowIndexColumnId());
            ResizeToContainIndex(&knownFields, systemColumnMaxId);
            knownFields[GetTableIndexColumnId()] = TSkiffEncodingInfo::Skip();
            knownFields[GetRangeIndexColumnId()] = TSkiffEncodingInfo::Skip();
            if (commonTableDescription.RangeIndexFieldIndex) {
                knownFields[GetRangeIndexColumnId()] = TSkiffEncodingInfo::RangeIndex(*commonTableDescription.RangeIndexFieldIndex);
            } else {
                knownFields[GetRangeIndexColumnId()] = TSkiffEncodingInfo::Skip();
            }
            if (commonTableDescription.RowIndexFieldIndex) {
                knownFields[GetRowIndexColumnId()] = TSkiffEncodingInfo::RowIndex(*commonTableDescription.RowIndexFieldIndex);
            } else {
                knownFields[GetRowIndexColumnId()] = TSkiffEncodingInfo::Skip();
            }
        }
    }

private:
    std::vector<TErrorAttribute> GetRowPositionErrorAttributes() const
    {
        if (CurrentRow_ == nullptr) {
            return {};
        }

        i64 tableIndex = 0;
        std::optional<i64> rowIndex;

        // We don't use tableIndex / rowIndex from DoWrite function because sometimes we want
        // to throw error before DoWrite knows table index / row index.
        // To keep things simple we always recompute table index / row index by ourselves.
        for (const auto& value : *CurrentRow_) {
            if (value.Id == GetTableIndexColumnId()) {
                YCHECK(value.Type == EValueType::Int64);
                tableIndex = value.Data.Int64;
            } else if (value.Id == GetRowIndexColumnId()) {
                YCHECK(value.Type == EValueType::Int64);
                rowIndex = value.Data.Int64;
            }
        }

        std::vector<TErrorAttribute> result = {
            TErrorAttribute("table_index", tableIndex),
        };
        if (rowIndex) {
            result.emplace_back("row_index", *rowIndex);
        }
        return result;
    }

    virtual void DoWrite(TRange<TUnversionedRow> rows) override
    {
        const auto rowCount = rows.Size();
        for (size_t rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            const auto& row = rows[rowIndex];
            CurrentRow_ = &row;
            auto finallyGuard = Finally([&] {
                CurrentRow_ = nullptr;
            });

            const auto valueCount = row.GetCount();
            ui32 tableIndex = 0;
            for (const auto& value : row) {
                if (value.Id == GetTableIndexColumnId()) {
                    tableIndex = value.Data.Int64;
                    break;
                }
            }
            if (tableIndex >= TableDescriptionList_.size()) {
                THROW_ERROR_EXCEPTION("Table #%v is not described by Skiff schema",
                    tableIndex)
                    << GetRowPositionErrorAttributes();
            }

            const auto& knownFields = TableDescriptionList_[tableIndex].KnownFields;
            const auto& denseFields = TableDescriptionList_[tableIndex].DenseFieldInfos;
            const auto hasOtherColumns = TableDescriptionList_[tableIndex].HasOtherColumns;
            const auto keySwitchFieldIndex = TableDescriptionList_[tableIndex].KeySwitchFieldIndex;
            const auto rowIndexFieldIndex = TableDescriptionList_[tableIndex].RowIndexFieldIndex;
            const auto rangeIndexFieldIndex = TableDescriptionList_[tableIndex].RangeIndexFieldIndex;

            const bool isLastRowInBatch = rowIndex + 1 == rowCount;

            constexpr ui16 missingColumnPlaceholder = ui16(-1);
            constexpr ui16 keySwitchColumnPlaceholder = ui16(-2);
            DenseIndexes_.assign(denseFields.size(), missingColumnPlaceholder);
            SparseFields_.clear();
            OtherValueIndexes_.clear();

            if (keySwitchFieldIndex != MissingSystemColumn) {
                DenseIndexes_[keySwitchFieldIndex] = keySwitchColumnPlaceholder;
            }

            ui32 rowIndexValueId = -1;
            ui32 rangeIndexValueId = -1;

            for (ui32 valueIndex = 0; valueIndex < valueCount; ++valueIndex) {
                const auto& value = row[valueIndex];

                const auto columnId = value.Id;
                static const TSkiffEncodingInfo unknownField = TSkiffEncodingInfo();
                const auto& encodingInfo = columnId < knownFields.size() ? knownFields[columnId] : unknownField;
                switch (encodingInfo.EncodingPart) {
                    case ESkiffWriterColumnType::Dense:
                        DenseIndexes_[encodingInfo.FieldIndex] = valueIndex;
                        break;
                    case ESkiffWriterColumnType::Sparse:
                        Y_ASSERT(encodingInfo.Required == true);
                        SparseFields_.emplace_back(
                            encodingInfo.WireType,
                            encodingInfo.FieldIndex,
                            valueIndex);
                        break;
                    case ESkiffWriterColumnType::Skip:
                        break;
                    case ESkiffWriterColumnType::RowIndex:
                        rowIndexValueId = valueIndex;
                        break;
                    case ESkiffWriterColumnType::RangeIndex:
                        rangeIndexValueId = valueIndex;
                        break;
                    case ESkiffWriterColumnType::Unknown:
                        if (!hasOtherColumns) {
                            THROW_ERROR_EXCEPTION("Column %Qv is not described by Skiff schema and there is no %Qv column",
                                NameTable_->GetName(columnId),
                                OtherColumnsName)
                                << GetRowPositionErrorAttributes();
                        }
                        OtherValueIndexes_.emplace_back(valueIndex);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            }
            if (rowIndexFieldIndex != MissingSystemColumn || rangeIndexFieldIndex != MissingSystemColumn) {
                bool needUpdateRangeIndex = tableIndex != TableIndex_;
                if (rangeIndexValueId != static_cast<ui32>(-1)) {
                    YCHECK(row[rangeIndexValueId].Type == EValueType::Int64);
                    const auto rangeIndex = row[rangeIndexValueId].Data.Int64;
                    needUpdateRangeIndex = needUpdateRangeIndex || rangeIndex != RangeIndex_;
                    if (rangeIndexFieldIndex != MissingSystemColumn) {
                        if (needUpdateRangeIndex) {
                            DenseIndexes_[rangeIndexFieldIndex] = rangeIndexValueId;
                        }
                    }
                    RangeIndex_ = rangeIndex;
                } else if (rangeIndexFieldIndex != MissingSystemColumn) {
                    THROW_ERROR_EXCEPTION("Range index requested but reader did not return it")
                        << GetRowPositionErrorAttributes();
                }
                if (rowIndexValueId != static_cast<ui32>(-1)) {
                    YCHECK(row[rowIndexValueId].Type == EValueType::Int64);
                    const auto rowIndex = row[rowIndexValueId].Data.Int64;
                    bool needUpdateRowIndex = needUpdateRangeIndex || rowIndex != RowIndex_ + 1;
                    if (rowIndexFieldIndex != MissingSystemColumn) {
                        if (needUpdateRowIndex) {
                            DenseIndexes_[rowIndexFieldIndex] = rowIndexValueId;
                        }
                    }
                    RowIndex_ = rowIndex;
                } else if (rowIndexFieldIndex != MissingSystemColumn) {
                    THROW_ERROR_EXCEPTION("Row index requested but reader did not return it")
                        << GetRowPositionErrorAttributes();
                }
                TableIndex_ = tableIndex;
            }

            SkiffWriter_->WriteVariant16Tag(tableIndex);
            for (size_t idx = 0; idx < denseFields.size(); ++idx) {
                const auto& fieldInfo = denseFields[idx];
                const auto valueIndex = DenseIndexes_[idx];

                switch (valueIndex) {
                    case missingColumnPlaceholder:
                        if (fieldInfo.Required) {
                            auto value = MakeUnversionedSentinelValue(EValueType::Null, fieldInfo.ColumnId);
                            // WriteValue succeeds iff fieldInfo.WireType is Yson32 otherwise it fails
                            WriteValue(fieldInfo.WireType, value);
                        } else {
                            SkiffWriter_->WriteVariant8Tag(0);
                        }
                        break;
                    case keySwitchColumnPlaceholder:
                        SkiffWriter_->WriteBoolean(CheckKeySwitch(row, isLastRowInBatch));
                        break;
                    default: {
                        const auto& value = row[valueIndex];
                        if (!fieldInfo.Required) {
                            if (value.Type == EValueType::Null) {
                                SkiffWriter_->WriteVariant8Tag(0);
                                continue;
                            } else {
                                SkiffWriter_->WriteVariant8Tag(1);
                            }
                        }
                        WriteValue(fieldInfo.WireType, value);
                        break;
                    }
                }
            }

            if (!SparseFields_.empty()) {
                for (const auto& fieldInfo : SparseFields_) {
                    const auto& value = row[fieldInfo.ValueIndex];
                    if (value.Type != EValueType::Null) {
                        SkiffWriter_->WriteVariant16Tag(fieldInfo.SparseFieldIndex);
                        WriteValue(fieldInfo.WireType, value);
                    }
                }
                SkiffWriter_->WriteVariant16Tag(EndOfSequenceTag<ui16>());
            }
            if (hasOtherColumns) {
                YsonBuffer_.clear();
                TStringOutput out(YsonBuffer_);
                NYson::TYsonWriter writer(&out);
                writer.OnBeginMap();
                for (const auto otherValueIndex : OtherValueIndexes_) {
                    const auto& value = row[otherValueIndex];
                    writer.OnKeyedItem(NameTable_->GetName(value.Id));
                    WriteYsonValue(&writer, value);
                }
                writer.OnEndMap();
                SkiffWriter_->WriteYson32(YsonBuffer_);
            }
            SkiffWriter_->Flush();
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

    Y_FORCE_INLINE void WriteValue(NSkiff::EWireType wireType, const TUnversionedValue& value)
    {
        switch (wireType) {
            case EWireType::Int64:
                ValidateType(EValueType::Int64, value.Type, value.Id);
                SkiffWriter_->WriteInt64(value.Data.Int64);
                break;
            case EWireType::Uint64:
                ValidateType(EValueType::Uint64, value.Type, value.Id);
                SkiffWriter_->WriteUint64(value.Data.Uint64);
                break;
            case EWireType::Boolean:
                ValidateType(EValueType::Boolean, value.Type, value.Id);
                SkiffWriter_->WriteBoolean(value.Data.Boolean);
                break;
            case EWireType::Double:
                ValidateType(EValueType::Double, value.Type, value.Id);
                SkiffWriter_->WriteDouble(value.Data.Double);
                break;
            case EWireType::String32:
                ValidateType(EValueType::String, value.Type, value.Id);
                SkiffWriter_->WriteString32(TStringBuf(value.Data.String, value.Length));
                break;
            case EWireType::Yson32: {
                YsonBuffer_.clear();
                TStringOutput out(YsonBuffer_);

                NYson::TYsonWriter writer(&out);
                WriteYsonValue(&writer, value);
                SkiffWriter_->WriteYson32(YsonBuffer_);
                break;
            }
            default:
                Y_UNREACHABLE();
        }
    }

    Y_FORCE_INLINE void ValidateType(EValueType expected, EValueType actual, ui16 columnId)
    {
        if (expected != actual) {
            THROW_ERROR_EXCEPTION("Unexpected type of %Qv column, expected %Qlv found %Qlv",
                NameTable_->GetName(columnId),
                expected,
                actual)
                << GetRowPositionErrorAttributes();
        }
    }

private:
    using TSkiffEncodingInfoList = std::vector<TSkiffEncodingInfo>;

    const NSkiff::TSkiffSchemaPtr SkiffSchema_;
    std::optional<NSkiff::TCheckedInDebugSkiffWriter> SkiffWriter_;

    std::vector<ui16> DenseIndexes_;
    std::vector<TSparseFieldInfo> SparseFields_;
    std::vector<ui16> OtherValueIndexes_;

    // Table #i is described by element with index i.
    std::vector<TSkiffWriterTableDescription> TableDescriptionList_;

    i64 TableIndex_ = -1;
    i64 RangeIndex_ = -1;
    i64 RowIndex_ = -1;

    // Buffer that we are going to reuse in order to reduce memory allocations.
    TString YsonBuffer_;

    const TUnversionedRow* CurrentRow_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForSkiff(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = NYTree::ConvertTo<TSkiffFormatConfigPtr>(attributes);
    auto skiffSchemas = ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);
    return CreateSchemalessWriterForSkiff(
        skiffSchemas,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount
    );
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForSkiff(
    const std::vector<TSkiffSchemaPtr>& tableSkiffSchemas,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto result = New<TSkiffSchemalessWriter>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount
    );
    result->Init(tableSkiffSchemas);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
