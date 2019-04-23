#include "skiff_writer.h"
#include "config.h"

#include "schemaless_writer_adapter.h"
#include "skiff_yson_converter.h"

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/finally.h>

#include <yt/core/skiff/skiff.h>
#include <yt/core/skiff/skiff_schema.h>

#include <yt/core/skiff/schema_match.h>

#include <yt/core/yson/pull_parser.h>
#include <yt/core/yson/writer.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>

#include <functional>

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
)

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ResizeToContainIndex(std::vector<T>* vec, size_t index)
{
    if (vec->size() < index + 1) {
        vec->resize(index + 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TIndexedSchemas
{
public:
    explicit TIndexedSchemas(const std::vector<TTableSchema>& tableSchemas)
    {
        for (size_t tableIndex = 0; tableIndex < tableSchemas.size(); ++tableIndex) {
            const auto& columns = tableSchemas[tableIndex].Columns();
            for (const auto& column : columns) {
                Columns_[std::pair<int,TString>(tableIndex, column.Name())] = column;
            }
        }
    }

    const TColumnSchema* GetColumnSchema(int tableIndex, TStringBuf columnName) const
    {
        auto it = Columns_.find(std::pair<int,TString>(tableIndex, columnName));
        if (it == Columns_.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

private:
    // (TableIndex, ColumnName) -> ColumnSchema
    THashMap<std::pair<int, TString>, TColumnSchema> Columns_;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteContext
{
    TNameTablePtr NameTable;
    TBuffer* TmpBuffer = nullptr;
};

using TUnversionedValueToSkiffConverter = std::function<void(const TUnversionedValue&, TCheckedInDebugSkiffWriter*, TWriteContext*)>;

template <EWireType wireType>
constexpr EValueType WireTypeToValueType()
{
    if constexpr (wireType == EWireType::Int64) {
        return EValueType::Int64;
    } else if constexpr (wireType == EWireType::Uint64) {
        return EValueType::Uint64;
    } else if constexpr (wireType == EWireType::Double) {
        return EValueType::Double;
    } else if constexpr (wireType == EWireType::Boolean) {
        return EValueType::Boolean;
    } else if constexpr (wireType == EWireType::String32) {
        return EValueType::String;
    } else {
        // Not compilable.
        static_assert(wireType == EWireType::Int64, "Bad wireType");
    }
}

template<EWireType wireType, bool required>
void ConvertSimpleValueImpl(const TUnversionedValue& value, TCheckedInDebugSkiffWriter* writer, TWriteContext* context)
{
    if constexpr (!required) {
        if (value.Type == EValueType::Null) {
            writer->WriteVariant8Tag(0);
            return;
        } else {
            writer->WriteVariant8Tag(1);
        }
    }

    if constexpr (wireType != EWireType::Yson32) {
        constexpr auto expectedValueType = WireTypeToValueType<wireType>();
        if (value.Type != expectedValueType) {
            THROW_ERROR_EXCEPTION("Unexpected type of %Qv column, expected: %Qlv found %Qlv",
                context->NameTable->GetName(value.Id),
                expectedValueType,
                value.Type);
        }
    }

    if constexpr (wireType == EWireType::Int64) {
        writer->WriteInt64(value.Data.Int64);
    } else if constexpr (wireType == EWireType::Uint64) {
        writer->WriteUint64(value.Data.Uint64);
    } else if constexpr (wireType == EWireType::Boolean) {
        writer->WriteBoolean(value.Data.Boolean);
    } else if constexpr (wireType == EWireType::Double) {
        writer->WriteDouble(value.Data.Double);
    } else if constexpr (wireType == EWireType::String32) {
        writer->WriteString32(TStringBuf(value.Data.String, value.Length));
    } else if constexpr (wireType == EWireType::Yson32) {
        context->TmpBuffer->Clear();
        {
            TBufferOutput out(*context->TmpBuffer);
            NYson::TYsonWriter writer(&out);
            WriteYsonValue(&writer, value);
        }
        writer->WriteYson32(TStringBuf(context->TmpBuffer->data(), context->TmpBuffer->size()));
    } else {
        // Not compilable.
        static_assert(wireType == EWireType::Int64, "Bad wireType");
    }
}

TUnversionedValueToSkiffConverter CreateSimpleValueConverter(EWireType wireType, bool required)
{
    switch (wireType) {
#define CASE(t) \
        case t: \
            return required ? ConvertSimpleValueImpl<t, true> : ConvertSimpleValueImpl<t, false>;
        CASE(EWireType::Int64)
        CASE(EWireType::Uint64)
        CASE(EWireType::Double)
        CASE(EWireType::Boolean)
        CASE(EWireType::String32)
        CASE(EWireType::Yson32)
#undef CASE

        default:
            Y_UNREACHABLE();
    }
}

TUnversionedValueToSkiffConverter CreateComplexValueConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    bool isSparse)
{
    TYsonToSkiffConverterConfig config;
    config.ExpectTopLevelOptionalSet = isSparse;
    auto ysonToSkiff = CreateYsonToSkiffConverter(std::move(descriptor), skiffSchema, config);
    return [ysonToSkiff=ysonToSkiff] (const TUnversionedValue& value, TCheckedInDebugSkiffWriter* skiffWriter, TWriteContext* context) {
        TMemoryInput input;
        if (value.Type == EValueType::Any) {
            input.Reset(value.Data.String, value.Length);
        } else if (value.Type == EValueType::Null) {
            static const auto empty = AsStringBuf("#");
            input.Reset(empty.Data(), empty.Size());
        } else {
            THROW_ERROR_EXCEPTION("Internal error: unexpected value type; expected: %Qlv or %Qlv actual: %Qlv",
                EValueType::Any,
                EValueType::Null,
                value.Type);
        }
        NYson::TYsonPullParser parser(&input, NYson::EYsonType::Node);
        NYson::TYsonPullParserCursor cursor(&parser);
        ysonToSkiff(&cursor, skiffWriter);
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TSkiffEncodingInfo
{
    ESkiffWriterColumnType EncodingPart = ESkiffWriterColumnType::Unknown;

    // Convereter is set only for sparse part.
    TUnversionedValueToSkiffConverter Converter;

    // FieldIndex is index of field inside skiff tuple for dense part of the row
    // and variant tag for sparse part of the row.
    ui32 FieldIndex = 0;

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

    static TSkiffEncodingInfo Dense(ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Dense;
        result.FieldIndex = fieldIndex;
        return result;
    }

    static TSkiffEncodingInfo Sparse(TUnversionedValueToSkiffConverter converter, ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Sparse;
        result.Converter = converter;
        result.FieldIndex = fieldIndex;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////


struct TSparseFieldInfo
{
    const TUnversionedValueToSkiffConverter* Converter;
    ui32 SparseFieldTag;
    ui32 ValueIndex;

    TSparseFieldInfo(const TUnversionedValueToSkiffConverter* converter, ui32 sparseFieldTag, ui32 valueIndex)
        : Converter(converter)
        , SparseFieldTag(sparseFieldTag)
        , ValueIndex(valueIndex)
    { }
};

struct TDenseFieldWriterInfo
{
    TUnversionedValueToSkiffConverter Converter;
    ui16 ColumnId;

    TDenseFieldWriterInfo(const TUnversionedValueToSkiffConverter& converter, ui16 columnId)
        : Converter(converter)
        , ColumnId(columnId)
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

class TSkiffWriter
    : public TSchemalessFormatWriterBase
{
public:
    TSkiffWriter(
        TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount)
        : TSchemalessFormatWriterBase(
            std::move(nameTable),
            std::move(output),
            enableContextSaving,
            std::move(controlAttributesConfig),
            keyColumnCount)
    { }

    void Init(const std::vector<TTableSchema>& schemas, const std::vector<TSkiffSchemaPtr>& tableSkiffSchemas)
    {
        auto streamSchema = CreateVariant16Schema(tableSkiffSchemas);
        SkiffWriter_.emplace(streamSchema, GetOutputStream());

        auto indexedSchemas = TIndexedSchemas(schemas);

        auto tableDescriptionList = CreateTableDescriptionList(tableSkiffSchemas, RangeIndexColumnName, RowIndexColumnName);
        for (const auto& commonTableDescription : tableDescriptionList) {
            auto tableIndex = TableDescriptionList_.size();
            TableDescriptionList_.emplace_back();
            auto& writerTableDescription = TableDescriptionList_.back();
            writerTableDescription.HasOtherColumns = commonTableDescription.HasOtherColumns;
            writerTableDescription.KeySwitchFieldIndex = commonTableDescription.KeySwitchFieldIndex.value_or(MissingSystemColumn);
            writerTableDescription.RowIndexFieldIndex = commonTableDescription.RowIndexFieldIndex.value_or(MissingSystemColumn);
            writerTableDescription.RangeIndexFieldIndex = commonTableDescription.RangeIndexFieldIndex.value_or(MissingSystemColumn);

            auto& knownFields = writerTableDescription.KnownFields;

            const auto& denseFieldDescriptionList = commonTableDescription.DenseFieldDescriptionList;

            auto& denseFieldWriterInfos = writerTableDescription.DenseFieldInfos;

            auto createComplexValueConverter = [&] (const TFieldDescription& field, bool isSparse) -> std::optional<TUnversionedValueToSkiffConverter> {
                auto columnSchema = indexedSchemas.GetColumnSchema(tableIndex, field.Name());

                if (!columnSchema || columnSchema->SimplifiedLogicalType()) {
                    // NB: we don't create complex value converter for simple types:
                    //   1. Complex value converter expects unversioned values of type ANY and simple types have other types.
                    //   2. For historical reasons we don't check skiff schema that strictly for simple types,
                    //      e.g we allow column to be optional in table schema and be required in skiff schema
                    //      (runtime check is used in such cases).
                    return {};
                }
                auto descriptor = TComplexTypeFieldDescriptor(field.Name(), columnSchema->LogicalType());
                return CreateComplexValueConverter(std::move(descriptor), field.Schema(), isSparse);
            };

            for (size_t i = 0; i < denseFieldDescriptionList.size(); ++i) {
                const auto& denseField = denseFieldDescriptionList[i];
                const auto id = NameTable_->GetIdOrRegisterName(denseField.Name());
                ResizeToContainIndex(&knownFields, id);
                YCHECK(knownFields[id].EncodingPart == ESkiffWriterColumnType::Unknown);
                knownFields[id] = TSkiffEncodingInfo::Dense(i);

                auto simplified = denseField.Simplify();
                TUnversionedValueToSkiffConverter converter;
                if (auto complexConverter = createComplexValueConverter(denseField, /*sparse*/ false)) {
                    converter = *complexConverter;
                } else {
                    converter = CreateSimpleValueConverter(*simplified, denseField.IsRequired());
                }
                denseFieldWriterInfos.emplace_back(converter, id);
            }

            const auto& sparseFieldDescriptionList = commonTableDescription.SparseFieldDescriptionList;
            for (size_t i = 0; i < sparseFieldDescriptionList.size(); ++i) {
                const auto& sparseField = sparseFieldDescriptionList[i];
                auto id = NameTable_->GetIdOrRegisterName(sparseField.Name());
                ResizeToContainIndex(&knownFields, id);
                YCHECK(knownFields[id].EncodingPart == ESkiffWriterColumnType::Unknown);

                TUnversionedValueToSkiffConverter converter;
                if (auto complexConverter = createComplexValueConverter(sparseField, /*sparse*/ true)) {
                    converter = *complexConverter;
                } else {
                    converter = CreateSimpleValueConverter(sparseField.ValidatedSimplify(), true);
                }
                knownFields[id] = TSkiffEncodingInfo::Sparse(converter, i);
            }

            const auto systemColumnMaxId = Max(GetTableIndexColumnId(), GetRangeIndexColumnId(), GetRowIndexColumnId());
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
        TWriteContext writeContext;
        writeContext.NameTable = NameTable_;
        writeContext.TmpBuffer = &YsonBuffer_;

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
                        SparseFields_.emplace_back(
                            &encodingInfo.Converter,
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
                        fieldInfo.Converter(
                            MakeUnversionedSentinelValue(EValueType::Null, fieldInfo.ColumnId),
                            &*SkiffWriter_,
                            &writeContext);
                        break;
                    case keySwitchColumnPlaceholder:
                        SkiffWriter_->WriteBoolean(CheckKeySwitch(row, isLastRowInBatch));
                        break;
                    default: {
                        const auto& value = row[valueIndex];
                        fieldInfo.Converter(
                            value,
                            &*SkiffWriter_,
                            &writeContext);
                        break;
                    }
                }
            }

            if (!SparseFields_.empty()) {
                for (const auto& fieldInfo : SparseFields_) {
                    const auto& value = row[fieldInfo.ValueIndex];
                    if (value.Type != EValueType::Null) {
                        SkiffWriter_->WriteVariant16Tag(fieldInfo.SparseFieldTag);
                        (*fieldInfo.Converter)(value, &*SkiffWriter_, &writeContext);
                    }
                }
                SkiffWriter_->WriteVariant16Tag(EndOfSequenceTag<ui16>());
            }
            if (hasOtherColumns) {
                YsonBuffer_.Clear();
                TBufferOutput out(YsonBuffer_);
                NYson::TYsonWriter writer(
                    &out,
                    NYson::EYsonFormat::Binary,
                    NYson::EYsonType::Node,
                    /* enableRaw */ true);
                writer.OnBeginMap();
                for (const auto otherValueIndex : OtherValueIndexes_) {
                    const auto& value = row[otherValueIndex];
                    writer.OnKeyedItem(NameTable_->GetName(value.Id));
                    WriteYsonValue(&writer, value);
                }
                writer.OnEndMap();
                SkiffWriter_->WriteYson32(TStringBuf(YsonBuffer_.Data(), YsonBuffer_.Size()));
            }
            SkiffWriter_->Flush();
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

private:
    using TSkiffEncodingInfoList = std::vector<TSkiffEncodingInfo>;

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
    TBuffer YsonBuffer_;

    const TUnversionedRow* CurrentRow_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchema>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = NYTree::ConvertTo<TSkiffFormatConfigPtr>(attributes);
    auto skiffSchemas = ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);
    return CreateWriterForSkiff(
        skiffSchemas,
        std::move(nameTable),
        schemas,
        std::move(output),
        enableContextSaving,
        std::move(controlAttributesConfig),
        keyColumnCount
    );
}

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const std::vector<TSkiffSchemaPtr>& tableSkiffSchemas,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchema>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto result = New<TSkiffWriter>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount
    );
    result->Init(schemas, tableSkiffSchemas);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
