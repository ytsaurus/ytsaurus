#include "state.h"

#include "serializer.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/flow/lib/delta_codecs/codec.h>
#include <yt/yt/flow/lib/delta_codecs/state.h>

#include <yt/yt/core/logging/log.h>

#include <util/generic/scope.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TString GetYsonStatePatchField(TStringBuf name)
{
    return Format("%v_patch", name);
}

////////////////////////////////////////////////////////////////////////////////

void TFormat::Register(TRegistrar registrar)
{
    registrar.Parameter("compression", &TThis::Compression)
        .Default(NCompression::ECodec::Zstd_6)
        .DontSerializeDefault();
    registrar.Parameter("patch_compression", &TThis::PatchCompression)
        .Default(NCompression::ECodec::None)
        .DontSerializeDefault();
    registrar.Parameter("delta", &TThis::Delta)
        .Default(NDeltaCodecs::ECodec::XDelta)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

TStateSchemaPtr BuildYsonStateSchema(std::function<TYsonStructPtr()> ctor)
{
    auto schema = New<TStateSchema>();
    schema->MakeYsonStruct = ctor;
    schema->IsEmpty = [empty = schema->MakeYsonStruct()](const TYsonStructPtr& value) {
        return empty->IsEqual(*value);
    };

    auto ysonStruct = schema->MakeYsonStruct();

    schema->YsonSchema = GetYsonSchema(ysonStruct);

    std::vector<TColumnSchema> columns;

    const auto* meta = ysonStruct->GetMeta();
    bool formatIsRequired = false;
    for (const auto& [key, parameter] : meta->GetParameterMap()) {
        if (key == StateFormatColumn) {
            THROW_ERROR_EXCEPTION("YsonState could not have column with name %Qv",
                StateFormatColumn);
        }
        TStringStream parameterSchema;
        TYsonWriter consumer(&parameterSchema);
        parameter->WriteSchema(ysonStruct.Get(), &consumer);
        consumer.Flush();
        auto logicalType = ConvertTo<TTypeV3LogicalTypeWrapper>(TYsonStringBuf(parameterSchema.Str())).LogicalType;
        auto [type, required] = CastToV1Type(logicalType);
        // State fields are always optional.
        required = false;
        auto columnType = parameter->FindOption<EYsonStateValueType>().value_or(EYsonStateValueType::Simple);
        switch (columnType) {
            case EYsonStateValueType::Simple: {
                columns.push_back(TColumnSchema(key, type).SetRequired(required));
                break;
            }
            case EYsonStateValueType::Compressed: {
                formatIsRequired = true;
                if (!IsStringLikeType(type)) {
                    THROW_ERROR_EXCEPTION("Compressed column should be string like type")
                        << TErrorAttribute("key", key)
                        << TErrorAttribute("type", type);
                }
                columns.push_back(TColumnSchema(key, ESimpleLogicalValueType::String).SetRequired(required));
                break;
            }
            case EYsonStateValueType::Packable: {
                formatIsRequired = true;
                if (!IsStringLikeType(type)) {
                    THROW_ERROR_EXCEPTION("Packable column should be string like type")
                        << TErrorAttribute("key", key)
                        << TErrorAttribute("type", type);
                }
                columns.push_back(TColumnSchema(key, ESimpleLogicalValueType::String).SetRequired(required));
                columns.push_back(TColumnSchema(GetYsonStatePatchField(key), ESimpleLogicalValueType::String).SetRequired(required));
                break;
            }
        }
    }
    if (formatIsRequired) {
        columns.push_back(TColumnSchema(std::string{StateFormatColumn}, ESimpleLogicalValueType::Any).SetRequired(false));
    }
    std::sort(columns.begin(), columns.end(), [] (const auto& l, const auto& r) {
        return l.Name() < r.Name();
    });

    schema->YsonToTableMapping.resize(schema->YsonSchema->GetColumnCount());
    schema->TableSchema = New<TTableSchema>(std::move(columns));
    for (const auto& [key, parameter] : meta->GetParameterMap()) {
        auto columnType = parameter->FindOption<EYsonStateValueType>().value_or(EYsonStateValueType::Simple);
        int ysonIndex = schema->YsonSchema->GetColumnIndex(key);
        int tableIndex = schema->TableSchema->GetColumnIndex(key);

        switch (columnType) {
            case EYsonStateValueType::Simple: {
                schema->YsonToTableMapping[ysonIndex] = {tableIndex, -1};
                break;
            }
            case EYsonStateValueType::Compressed: {
                schema->YsonToTableMapping[ysonIndex] = {tableIndex, -1};
                schema->CompressedColumns.insert(tableIndex);
                break;
            }
            case EYsonStateValueType::Packable: {
                auto tablePatchIndex = schema->TableSchema->GetColumnIndex(GetYsonStatePatchField(key));
                schema->YsonToTableMapping[ysonIndex] = {tableIndex, tablePatchIndex};
                schema->CompressedColumns.insert(tableIndex);
                schema->CompressedPatchColumns.insert(tablePatchIndex);
                break;
            }
        }
    }
    if (formatIsRequired) {
        schema->FormatColumn = schema->TableSchema->GetColumnIndex(StateFormatColumn);
    }

    return schema;
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

TState::TState(TStateSchemaPtr schema)
    : Schema_(std::move(schema))
{
    Init(std::nullopt);
}

void TState::Init(const std::optional<TUnversionedRow>& tableRow)
{
    if (tableRow) {
        std::vector<TUnversionedOwningValue> tableValues;
        tableValues.reserve(Schema_->TableSchema->GetColumnCount());
        for (int i = 0; i < Schema_->TableSchema->GetColumnCount(); ++i) {
            tableValues.push_back(MakeUnversionedNullValue(i));
        }
        for (const auto& value : *tableRow) {
            tableValues[value.Id] = value;
        }

        TableRow_ = std::move(tableValues);

        if (Schema_->FormatColumn) {
            auto value = FromUnversionedValue<std::optional<NYson::TYsonString>>((*TableRow_)[*Schema_->FormatColumn]);
            if (value) {
                Format_ = ConvertTo<TFormatPtr>(*value);
            } else {
                Format_ = New<TFormat>();
            }
        } else {
            Format_ = New<TFormat>();
        }
        ParseTableRow();
    } else {
        TableRow_ = std::nullopt;
        Format_ = New<TFormat>();
        UncompressedPackableTableColumns_.clear();
        TUnversionedOwningRowBuilder builder;
        for (int i = 0; i < Schema_->YsonSchema->GetColumnCount(); ++i) {
            builder.AddValue(MakeUnversionedNullValue(i));
        }
        YsonRow_ = builder.FinishRow();
    }
}

i64 TState::GetSize() const
{
    i64 size = 0;
    if (TableRow_) {
        size = TableRow_->capacity() * sizeof(TUnversionedOwningValue);
        for (TUnversionedValue v : *TableRow_) {
            size += IsStringLikeType(v.Type) ? std::ssize(v.AsStringBuf()) : 0;
        }
    }

    size += UncompressedPackableTableColumns_.size() * sizeof(decltype(*UncompressedPackableTableColumns_.begin()));
    for (const auto& state : GetValues(UncompressedPackableTableColumns_)) {
        if (state) {
            size += std::ssize(state->Base.ToStringBuf()) + std::ssize(state->Patch.ToStringBuf());
        }
    }
    size += YsonRow_.GetSpaceUsed();
    return size;
}

TFormatPtr TState::GetFormat() const
{
    return Format_;
}

void TState::ParseTableRow() {
    YT_VERIFY(Format_);
    YT_VERIFY(TableRow_);
    YT_VERIFY(std::ssize(*TableRow_) == Schema_->TableSchema->GetColumnCount());
    auto* codec = NCompression::GetCodec(Format_->Compression);
    auto* patchCodec = NCompression::GetCodec(Format_->PatchCompression);
    auto deltaCodec = NDeltaCodecs::GetCodec(Format_->Delta);
    TUnversionedOwningRowBuilder builder;
    for (int index = 0; index < std::ssize(Schema_->YsonToTableMapping); ++index) {
        auto [tableIndex, tablePatchIndex] = Schema_->YsonToTableMapping[index];

        const auto addValue = [&] (TUnversionedValue value) {
            value.Id = index;
            if (IsStringLikeType(value.Type)) {
                auto type = Schema_->YsonSchema->Columns()[index].GetWireType();
                YT_VERIFY(IsStringLikeType(type));
                value.Type = type;
            }
            builder.AddValue(value);
        };

        const auto extractString = [](const TUnversionedOwningValue& value) -> std::optional<TSharedRef> {
            if (value.Type() == EValueType::Null) {
                return std::nullopt;
            }
            YT_VERIFY(IsStringLikeType(value.Type()));
            return value.GetStringRef();
        };

        if (tablePatchIndex == -1) {
            if (Schema_->CompressedColumns.contains(tableIndex)) {
                auto compressed = extractString((*TableRow_)[tableIndex]);
                if (!compressed) {
                    addValue(MakeUnversionedNullValue());
                } else {
                    auto decompressed = Decompress(codec, *compressed);
                    addValue(MakeUnversionedStringValue(decompressed.ToStringBuf()));
                }
            } else {
                addValue((*TableRow_)[tableIndex]);
            }
        } else {
            const auto value = extractString((*TableRow_)[tableIndex]);
            const auto patch = extractString((*TableRow_)[tablePatchIndex]).value_or(TSharedRef::MakeEmpty());

            if (value) {
                auto deltaState = NDeltaCodecs::TState{
                    .Base = Decompress(codec, *value),
                    .Patch = Decompress(patchCodec, patch)
                };
                auto merged = deltaCodec->ApplyPatch(deltaState.Base, deltaState.Patch);
                addValue(MakeUnversionedStringValue(merged.ToStringBuf()));
                UncompressedPackableTableColumns_[index] = std::move(deltaState);
            } else {
                if (!patch.ToStringBuf().empty()) {
                    THROW_ERROR_EXCEPTION("Unexpected non-empty patch")
                        << TErrorAttribute("column", Schema_->TableSchema->Columns()[tablePatchIndex].Name());
                }
                addValue(MakeUnversionedNullValue());
                UncompressedPackableTableColumns_[index] = std::nullopt;
            }
        }
    }
    YsonRow_ = builder.FinishRow();
}

TYsonStructPtr TState::GetValue() const
{
    auto ysonStruct = Schema_->MakeYsonStruct();
    NYsonSerializer::Deserialize(ysonStruct, YsonRow_, Schema_->YsonSchema);
    return ysonStruct;
}

void TState::SetValue(TYsonStructPtr ysonStruct)
{
    NTableClient::TUnversionedOwningRow ysonRow;
    if (!ysonStruct || Schema_->IsEmpty(ysonStruct)) {
        TUnversionedOwningRowBuilder builder;
        for (int i = 0; i < Schema_->YsonSchema->GetColumnCount(); ++i) {
            builder.AddValue(MakeUnversionedNullValue(i));
        }
        ysonRow = builder.FinishRow();
    } else {
        ysonRow = NYsonSerializer::Serialize(ysonStruct, Schema_->YsonSchema);
    }

    std::swap(YsonRow_, ysonRow);

    for (int i = 0; i < YsonRow_.GetCount(); ++i) {
        if (YsonRow_[i].Type == ysonRow[i].Type && YsonRow_[i].Type == EValueType::Any) {
            if (YsonRow_[i].AsStringBuf() != ysonRow[i].AsStringBuf()) {
                ModifiedYsonRowColumns_.insert(i);
            }
        } else if (YsonRow_[i] != ysonRow[i]) {
            ModifiedYsonRowColumns_.insert(i);
        }
    }
}

void TState::SetFormat(TFormatPtr format)
{
    if (*Format_ != *format) {
        ForceRewrite();
    }
    Format_ = std::move(format);
}

void TState::ForceRewrite()
{
    Rewrite_ = true;
    for (int i = 0; i < YsonRow_.GetCount(); ++i) {
        ModifiedYsonRowColumns_.insert(i);
    }
}

TStateMutation TState::FlushMutation()
{
    auto finally = Finally([&] {
        ModifiedYsonRowColumns_.clear();
        Rewrite_ = false;
    });

    if (ModifiedYsonRowColumns_.empty() && !Rewrite_) {
        return {TEmptyMutation()};
    }

    if (IsEmptyYsonRow()) {
        if (TableRow_ || Rewrite_) {
            TableRow_ = std::nullopt;
            UncompressedPackableTableColumns_.clear();
            return {TEraseMutation()};
        } else {
            return {TEmptyMutation()};
        }
    }

    if (!TableRow_) {
        std::vector<TUnversionedOwningValue> tableValues;
        for (int i = 0; i < Schema_->TableSchema->GetColumnCount(); ++i) {
            tableValues.push_back(MakeUnversionedNullValue(i));
        }
        TableRow_ = std::move(tableValues);
    }

    YT_VERIFY(std::ssize(*TableRow_) == Schema_->TableSchema->GetColumnCount());

    TUnversionedOwningRowBuilder updateBuilder;

    NCompression::ICodec* codec = NCompression::GetCodec(Format_->Compression);
    NCompression::ICodec* patchCodec = NCompression::GetCodec(Format_->PatchCompression);

    if (Rewrite_ && Schema_->FormatColumn) {
        auto serializedFormat = ConvertToYsonString(Format_, EYsonFormat::Text);
        auto value = MakeUnversionedAnyValue(serializedFormat.AsStringBuf(), *Schema_->FormatColumn);
        (*TableRow_)[*Schema_->FormatColumn] = TUnversionedOwningValue(value);
        updateBuilder.AddValue((*TableRow_)[*Schema_->FormatColumn]);
    }

    for (const auto& index : ModifiedYsonRowColumns_) {
        const auto value = YsonRow_[index];
        const auto [tableIndex, tablePatchIndex] = Schema_->YsonToTableMapping[index];

        auto addValue = [&] (TUnversionedValue value) {
            value.Id = tableIndex;
            (*TableRow_)[tableIndex] = TUnversionedOwningValue(value);
            updateBuilder.AddValue((*TableRow_)[tableIndex]);
        };

        auto addPatch = [&] (TUnversionedValue patch) {
            patch.Id = tablePatchIndex;
            (*TableRow_)[tablePatchIndex] = TUnversionedOwningValue(patch);
            updateBuilder.AddValue((*TableRow_)[tablePatchIndex]);
        };

        if (tablePatchIndex == -1) {
            if (Schema_->CompressedColumns.contains(tableIndex) && codec) {
                if (value.Type == EValueType::Null) {
                    addValue(value);
                } else {
                    YT_VERIFY(IsStringLikeType(value.Type));
                    auto compressedValue = Compress(codec, TSharedRef::FromString(TString(value.AsStringBuf())));
                    addValue(MakeUnversionedStringValue(compressedValue.ToStringBuf()));
                }
            } else {
                addValue(value);
            }
        } else {
            auto& tableState = UncompressedPackableTableColumns_[index];
            if (value.Type == EValueType::Null) {
                if (tableState || Rewrite_) {
                    addValue(MakeUnversionedNullValue());
                    addPatch(MakeUnversionedNullValue());
                }
                tableState = std::nullopt;
            } else {
                YT_VERIFY(IsStringLikeType(value.Type));
                // TODO: remove copy
                auto valueRef = TSharedRef::FromString(TString(value.AsStringBuf()));
                bool emptyTableState = !tableState.has_value();
                if (!tableState) {
                    tableState = NDeltaCodecs::TState{
                        .Base = TSharedRef::MakeEmpty(),
                        .Patch = TSharedRef::MakeEmpty()
                    };
                }

                NDeltaCodecs::EAlgorithm algo = Rewrite_ ? NDeltaCodecs::EAlgorithm::ZeroPatch : NDeltaCodecs::EAlgorithm::SizeHeuristics;
                auto mutation = NDeltaCodecs::MutateState(NDeltaCodecs::GetCodec(Format_->Delta), *tableState, valueRef, algo);
                tableState = NDeltaCodecs::ApplyStateMutation(*tableState, mutation);
                if (mutation.Base || Rewrite_ || emptyTableState) {
                    auto compressedValue = Compress(codec, tableState->Base);
                    addValue(MakeUnversionedStringValue(compressedValue.ToStringBuf()));
                }
                if (mutation.Patch || Rewrite_ || emptyTableState) {
                    auto compressedPatch = Compress(patchCodec, tableState->Patch);
                    addPatch(MakeUnversionedStringValue(compressedPatch.ToStringBuf()));
                }
            }
        }
    }
    return {TUpdateMutation(updateBuilder.FinishRow())};
}

bool TState::IsEmptyYsonRow()
{
    for (const auto& value : YsonRow_) {
        if (value.Type != EValueType::Null) {
            return false;
        }
    }
    return true;
}

std::optional<TUnversionedOwningRow> TState::GetTableRow() const
{
    if (!TableRow_) {
        return std::nullopt;
    }

    TUnversionedOwningRowBuilder builder;
    for (const auto& value : *TableRow_) {
        builder.AddValue(TUnversionedValue(value));
    }
    return builder.FinishRow();
}

TTableSchemaPtr TState::GetTableSchema() const
{
    return Schema_->TableSchema;
}

TSharedRef TState::Compress(NCompression::ICodec* codec, const TSharedRef& value)
{
    if (value.ToStringBuf().empty()) {
        return value;
    }
    return codec->Compress(value);
}

TSharedRef TState::Decompress(NCompression::ICodec* codec, const TSharedRef& value)
{
    if (value.ToStringBuf().empty()) {
        return value;
    }
    return codec->Decompress(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
