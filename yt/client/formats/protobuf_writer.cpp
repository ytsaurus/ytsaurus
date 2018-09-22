#include "protobuf_writer.h"

#include "lenval_control_constants.h"
#include "protobuf.h"
#include "schemaless_writer_adapter.h"

#include <yt/client/table_client/name_table.h>

#include <yt/core/misc/varint.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::internal::WireFormatLite;

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
    {
        TableIndexToFieldIndexToDescription_.resize(Description_->GetTableCount());
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

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; ++index) {
            auto row = rows[index];

            if (CheckKeySwitch(row, index + 1 == rowCount)) {
                WritePod(*stream, LenvalKeySwitch);
            }

            WriteControlAttributes(row);

            std::vector<i64> enumValues;
            size_t enumIndex = 0;

            int size = 0;
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
                        case EProtobufType::EnumString:
                            if (value.Type == EValueType::Uint64) {
                                size += WireFormatLite::UInt32Size(value.Data.Uint64);
                            } else if (value.Type == EValueType::String) {
                                auto enumString = TStringBuf(value.Data.String, value.Length);
                                if (!fieldDescription->EnumerationDescription) {
                                    THROW_ERROR_EXCEPTION("Cannot serialize enumeration value %Qv since enumeration description is missing",
                                        enumString);
                                }
                                auto enumValue = fieldDescription->EnumerationDescription->GetValue(enumString);

                                enumValues.push_back(enumValue);
                                size += WireFormatLite::Int32Size(enumValue);
                            } else {
                                THROW_ERROR_EXCEPTION("Protobuf enums can be stored only in \"uint64\" or \"string\" column");
                            }
                            break;
                        case EProtobufType::Message:
                            ValidateType(value, EValueType::String);
                            size += WireFormatLite::UInt32Size(value.Length);
                            size += value.Length;
                            break;
                        default:
                            Y_UNREACHABLE();
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error writing value of field %Qv",
                        fieldDescription->Name)
                        << ex;
                }
            }

            WritePod(*stream, static_cast<ui32>(size));

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
                    case EProtobufType::EnumString:
                        if (value.Type == EValueType::Int64) {
                            WriteVarUint64(stream, value.Data.Int64);
                        } else if (value.Type == EValueType::Uint64) {
                            WriteVarUint64(stream, value.Data.Uint64);
                        } else if (value.Type == EValueType::String) {
                            WriteVarUint64(stream, enumValues[enumIndex]);
                            ++enumIndex;
                        } else {
                            Y_UNREACHABLE();
                        }
                        break;
                    case EProtobufType::Message:
                        WriteVarUint32(stream, value.Length);
                        stream->Write(value.Data.String, value.Length);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            }

            TryFlushBuffer(false);
        }

        TryFlushBuffer(true);
    }

    virtual void WriteTableIndex(i64 tableIndex) override
    {
        CurrentTableIndex_ = tableIndex;

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
        YCHECK(fieldIndex < static_cast<ui32>(nameTable->GetSize()));
        if (fieldIndexToDescription.size() <= fieldIndex) {
            fieldIndexToDescription.reserve(fieldIndex + 1);
            for (size_t i = fieldIndexToDescription.size(); i <= fieldIndex; ++i) {
                Y_ASSERT(fieldIndexToDescription.size() == i);
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

private:
    const TProtobufFormatDescriptionPtr Description_;
    std::vector<std::vector<const TProtobufFieldDescription*>> TableIndexToFieldIndexToDescription_;

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

} // namespace NFormats
} // namespace NYT
