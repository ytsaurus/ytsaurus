#include "protobuf_writer.h"

#include "lenval_control_constants.h"
#include "protobuf.h"
#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/misc/varint.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

// This class is responsible for writing "other columns" field in protobuf format.
//
// |OnBeginRow|, |OnValue|, |OnEndRow|, |GetProtobufSize| and |WriteProtoField|
// methods can be called even if there is no "other columns" field in current table descriptor,
// in which case they will be effectively no-op.
class TOtherColumnsWriter
{
public:
    TOtherColumnsWriter(
        const TNameTablePtr& nameTable,
        const TProtobufFormatDescriptionPtr& description)
        : NameTable_(nameTable)
        , Writer_(
            &OutputStream_,
            EYsonFormat::Binary,
            EYsonType::Node,
            /* enableRaw */ true)
    {
        TableIndexToOtherColumnsField_.resize(description->GetTableCount());
        for (size_t tableIndex = 0; tableIndex < description->GetTableCount(); ++tableIndex) {
            const auto& tableDescription = description->GetTableDescription(tableIndex);
            for (const auto& [name, fieldDescription] : tableDescription.Columns) {
                if (fieldDescription.Type == EProtobufType::OtherColumns) {
                    TableIndexToOtherColumnsField_[tableIndex] = &fieldDescription;
                    break;
                }
            }
        }
    }

    void SetTableIndex(i64 tableIndex)
    {
        YT_VERIFY(!InsideRow_);
        FieldDescription_ = TableIndexToOtherColumnsField_[tableIndex];
    }

    bool IsEnabled() const
    {
        return FieldDescription_ != nullptr;
    }

    void OnBeginRow()
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(!InsideRow_);
        OutputStream_.Clear();
        Writer_.OnBeginMap();
        InsideRow_ = true;
    }

    void OnValue(TUnversionedValue value)
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(InsideRow_);
        Writer_.OnKeyedItem(NameTable_->GetName(value.Id));
        Serialize(value, &Writer_, /* anyAsRaw */ true);
    }

    void OnEndRow()
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(InsideRow_);
        Writer_.OnEndMap();
        InsideRow_ = false;
    }

    int GetProtobufSize() const
    {
        if (!IsEnabled()) {
            return 0;
        }

        YT_VERIFY(!InsideRow_);
        auto length = GetYsonString().size();
        return FieldDescription_->TagSize + WireFormatLite::UInt32Size(length) + length;
    }

    void WriteProtoField(IOutputStream* stream) const
    {
        if (!IsEnabled()) {
            return;
        }

        YT_VERIFY(!InsideRow_);
        WriteVarUint32(stream, FieldDescription_->WireTag);
        auto buffer = GetYsonString();
        WriteVarUint32(stream, buffer.size());
        stream->Write(buffer.begin(), buffer.size());
    }

private:
    TStringBuf GetYsonString() const
    {
        return OutputStream_.Blob().ToStringBuf();
    }

private:
    const TNameTablePtr NameTable_;

    std::vector<const TProtobufFieldDescription*> TableIndexToOtherColumnsField_;
    const TProtobufFieldDescription* FieldDescription_ = nullptr;

    TBlobOutput OutputStream_;
    TYsonWriter Writer_;

    bool InsideRow_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForProtobuf
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterForProtobuf(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount,
        TProtobufFormatDescriptionPtr description)
        : TSchemalessFormatWriterBase(
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount)
        , Description_(description)
        , OtherColumnsWriter_(nameTable, description)
    {
        TableIndexToFieldIndexToDescription_.resize(Description_->GetTableCount());
        OtherColumnsWriter_.SetTableIndex(CurrentTableIndex_);
    }

private:
    void ValidateType(const TUnversionedValue& value, EValueType type)
    {
        if (Y_UNLIKELY(value.Type != type)) {
            THROW_ERROR_EXCEPTION("Invalid protobuf storage type: expected %Qlv, got %Qlv",
                type,
                value.Type);
        }
    }

    virtual void DoWrite(TRange<TUnversionedRow> rows) override
    {
        auto* stream = GetOutputStream();

        std::vector<i32> enumValues;

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; ++index) {
            auto row = rows[index];

            if (CheckKeySwitch(row, index + 1 == rowCount)) {
                WritePod(*stream, LenvalKeySwitch);
            }

            WriteControlAttributes(row);

            OtherColumnsWriter_.OnBeginRow();

            enumValues.clear();
            size_t enumIndex = 0;

            int size = 0;
            for (const auto& value : row) {
                const auto* fieldDescription = GetFieldDescription(
                    CurrentTableIndex_,
                    value.Id,
                    NameTable_);
                if (!fieldDescription) {
                    OtherColumnsWriter_.OnValue(value);
                }

                if (!fieldDescription || value.Type == EValueType::Null) {
                    continue;
                }

                size += fieldDescription->TagSize;

                try {
                    switch (fieldDescription->Type) {
                        case EProtobufType::String:
                        case EProtobufType::Bytes:
                            ValidateType(value, EValueType::String);
                            size += WireFormatLite::UInt32Size(value.Length);
                            size += value.Length;
                            break;
                        case EProtobufType::Uint64:
                            ValidateType(value, EValueType::Uint64);
                            size += WireFormatLite::UInt64Size(value.Data.Uint64);
                            break;
                        case EProtobufType::Uint32:
                            ValidateType(value, EValueType::Uint64);
                            size += WireFormatLite::UInt32Size(value.Data.Uint64);
                            break;
                        case EProtobufType::Int64:
                            ValidateType(value, EValueType::Int64);
                            size += WireFormatLite::Int64Size(value.Data.Int64);
                            break;
                        case EProtobufType::Int32:
                            ValidateType(value, EValueType::Int64);
                            size += WireFormatLite::Int32Size(value.Data.Int64);
                            break;
                        case EProtobufType::Sint64:
                            ValidateType(value, EValueType::Int64);
                            size += WireFormatLite::SInt64Size(value.Data.Int64);
                            break;
                        case EProtobufType::Sint32:
                            ValidateType(value, EValueType::Int64);
                            size += WireFormatLite::SInt32Size(value.Data.Int64);
                            break;
                        case EProtobufType::Fixed64:
                            ValidateType(value, EValueType::Uint64);
                            size += sizeof(ui64);
                            break;
                        case EProtobufType::Fixed32:
                            ValidateType(value, EValueType::Uint64);
                            size += sizeof(ui32);
                            break;
                        case EProtobufType::Sfixed64:
                            ValidateType(value, EValueType::Int64);
                            size += sizeof(i64);
                            break;
                        case EProtobufType::Sfixed32:
                            ValidateType(value, EValueType::Int64);
                            size += sizeof(i32);
                            break;
                        case EProtobufType::Double:
                            ValidateType(value, EValueType::Double);
                            size += sizeof(double);
                            break;
                        case EProtobufType::Float:
                            ValidateType(value, EValueType::Double);
                            size += sizeof(float);
                            break;
                        case EProtobufType::Bool:
                            ValidateType(value, EValueType::Boolean);
                            size += 1;
                            break;
                        case EProtobufType::EnumInt:
                        case EProtobufType::EnumString: {
                            i32 enumValue;
                            switch (value.Type) {
                                case EValueType::Uint64: {
                                    auto inRange = TryIntegralCast<i32>(value.Data.Uint64, &enumValue);
                                    if (Y_UNLIKELY(!inRange)) {
                                        THROW_ERROR_EXCEPTION("uint64 value out of range for protobuf enumeration");
                                    }
                                    break;
                                }
                                case EValueType::Int64: {
                                    auto inRange = TryIntegralCast<i32>(value.Data.Int64, &enumValue);
                                    if (Y_UNLIKELY(!inRange)) {
                                        THROW_ERROR_EXCEPTION("int64 value out of range for protobuf enumeration");
                                    }
                                    break;
                                }
                                case EValueType::String: {
                                    auto enumString = TStringBuf(value.Data.String, value.Length);
                                    YT_VERIFY(fieldDescription->EnumerationDescription);
                                    enumValue = fieldDescription->EnumerationDescription->GetValue(enumString);
                                    enumValues.push_back(enumValue);
                                    break;
                                }
                                default:
                                    THROW_ERROR_EXCEPTION("Cannot parse protobuf enumeration %Qv from value of type %Qv",
                                        fieldDescription->EnumerationDescription->GetEnumerationName(),
                                        FormatEnum(value.Type));
                            }
                            size += WireFormatLite::Int32Size(enumValue);
                            break;
                        }
                        case EProtobufType::Message:
                            ValidateType(value, EValueType::String);
                            size += WireFormatLite::UInt32Size(value.Length);
                            size += value.Length;
                            break;
                        case EProtobufType::Any: {
                            auto& buffer = GetAnyColumnBuffer(value.Id);
                            buffer.Reserve(GetYsonSize(value));
                            TBufferOutput output(buffer);
                            TYsonWriter writer(&output, EYsonFormat::Binary, EYsonType::Node, /* enableRaw */ true);
                            UnversionedValueToYson(value, &writer);
                            size += WireFormatLite::UInt32Size(buffer.Size());
                            size += buffer.Size();
                            break;
                        }
                        default:
                            YT_ABORT();
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error writing value of field %Qv",
                        fieldDescription->Name)
                        << ex;
                }
            }

            OtherColumnsWriter_.OnEndRow();
            size += OtherColumnsWriter_.GetProtobufSize();

            WritePod(*stream, static_cast<ui32>(size));

            OtherColumnsWriter_.WriteProtoField(stream);

            for (const auto& value : row) {
                if (value.Type == EValueType::Null) {
                    continue;
                }

                const auto* fieldDescription = GetFieldDescription(
                    CurrentTableIndex_,
                    value.Id,
                    NameTable_);
                if (!fieldDescription) {
                    continue;
                }

                WriteVarUint32(stream, fieldDescription->WireTag);

                switch (fieldDescription->Type) {
                    case EProtobufType::String:
                    case EProtobufType::Bytes:
                        WriteVarUint32(stream, value.Length);
                        stream->Write(value.Data.String, value.Length);
                        break;
                    case EProtobufType::Uint64:
                        WriteVarUint64(stream, value.Data.Uint64);
                        break;
                    case EProtobufType::Uint32:
                        WriteVarUint32(stream, value.Data.Uint64);
                        break;
                    case EProtobufType::Int64:
                        WriteVarUint64(stream, value.Data.Int64); // no zigzag
                        break;
                    case EProtobufType::Int32:
                        WriteVarUint64(stream, value.Data.Int64); // no zigzag
                        break;
                    case EProtobufType::Sint64:
                        WriteVarInt64(stream, value.Data.Int64); // zigzag
                        break;
                    case EProtobufType::Sint32:
                        WriteVarInt32(stream, value.Data.Int64); // zigzag
                        break;
                    case EProtobufType::Fixed64:
                        WritePod(*stream, value.Data.Uint64);
                        break;
                    case EProtobufType::Fixed32:
                        WritePod(*stream, static_cast<ui32>(value.Data.Uint64));
                        break;
                    case EProtobufType::Sfixed64:
                        WritePod(*stream, value.Data.Int64);
                        break;
                    case EProtobufType::Sfixed32:
                        WritePod(*stream, static_cast<i32>(value.Data.Int64));
                        break;
                    case EProtobufType::Double:
                        WritePod(*stream, value.Data.Double);
                        break;
                    case EProtobufType::Float:
                        WritePod(*stream, static_cast<float>(value.Data.Double));
                        break;
                    case EProtobufType::Bool:
                        WritePod(*stream, static_cast<ui8>(value.Data.Boolean));
                        break;
                    case EProtobufType::EnumInt:
                    case EProtobufType::EnumString: {
                        i32 enumValue;
                        switch (value.Type) {
                            case EValueType::Uint64:
                                enumValue = static_cast<i32>(value.Data.Uint64);
                                break;
                            case EValueType::Int64:
                                enumValue = static_cast<i32>(value.Data.Int64);
                                break;
                            case EValueType::String:
                                enumValue = enumValues[enumIndex];
                                ++enumIndex;
                                break;
                            default:
                                // Must have thrown in the previous loop.
                                YT_ABORT();
                        }
                        WriteVarUint64(stream, enumValue); // No zigzag int32.
                        break;
                    }
                    case EProtobufType::Message:
                        WriteVarUint32(stream, value.Length);
                        stream->Write(value.Data.String, value.Length);
                        break;
                    case EProtobufType::Any: {
                        auto& buffer = GetAnyColumnBuffer(value.Id);
                        WriteVarUint32(stream, buffer.Size());
                        stream->Write(buffer.Data(), buffer.Size());
                        buffer.Clear();
                        break;
                    }
                    default:
                        YT_ABORT();
                }
            }

            TryFlushBuffer(false);
        }

        TryFlushBuffer(true);
    }

    virtual void WriteTableIndex(i64 tableIndex) override
    {
        CurrentTableIndex_ = tableIndex;
        OtherColumnsWriter_.SetTableIndex(tableIndex);

        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(LenvalTableIndexMarker));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    }

    virtual void WriteRangeIndex(i64 rangeIndex) override
    {
        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(LenvalRangeIndexMarker));
        WritePod(*stream, static_cast<ui32>(rangeIndex));
    }

    virtual void WriteRowIndex(i64 rowIndex) override
    {
        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(LenvalRowIndexMarker));
        WritePod(*stream, static_cast<ui64>(rowIndex));
    }

    const TProtobufFieldDescription* GetFieldDescription(ui32 tableIndex, ui32 fieldIndex, const NTableClient::TNameTablePtr& nameTable)
    {
        if (tableIndex >= TableIndexToFieldIndexToDescription_.size()) {
            THROW_ERROR_EXCEPTION("Table with index %v is missing in format description",
                tableIndex);
        }
        const auto& tableDescription = Description_->GetTableDescription(tableIndex);
        auto& fieldIndexToDescription = TableIndexToFieldIndexToDescription_[tableIndex];
        YT_VERIFY(fieldIndex < static_cast<ui32>(nameTable->GetSize()));
        if (fieldIndexToDescription.size() <= fieldIndex) {
            fieldIndexToDescription.reserve(fieldIndex + 1);
            for (size_t i = fieldIndexToDescription.size(); i <= fieldIndex; ++i) {
                YT_ASSERT(fieldIndexToDescription.size() == i);
                auto fieldName = nameTable->GetName(i);
                auto it = tableDescription.Columns.find(fieldName);
                if (it == tableDescription.Columns.end()) {
                    fieldIndexToDescription.push_back(nullptr);
                } else {
                    fieldIndexToDescription.push_back(&it->second);
                }
            }
        }
        return fieldIndexToDescription[fieldIndex];
    }

    TBuffer& GetAnyColumnBuffer(ui32 fieldIndex)
    {
        if (fieldIndex >= AnyColumnBuffers_.size()) {
            AnyColumnBuffers_.resize(fieldIndex + 1);
        }
        return AnyColumnBuffers_[fieldIndex];
    }

private:
    const TProtobufFormatDescriptionPtr Description_;
    std::vector<std::vector<const TProtobufFieldDescription*>> TableIndexToFieldIndexToDescription_;
    std::vector<TBuffer> AnyColumnBuffers_;
    TOtherColumnsWriter OtherColumnsWriter_;

    int CurrentTableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForProtobuf(
    TProtobufFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto description = New<TProtobufFormatDescription>();
    description->Init(config);
    return New<TSchemalessWriterForProtobuf>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        std::move(description));
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForProtobuf(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = ConvertTo<TProtobufFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForProtobuf(
        config,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
