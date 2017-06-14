#include "protobuf_writer.h"

#include "protobuf.h"
#include "schemaless_writer_adapter.h"

#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/misc/varint.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
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
        TProtobufFormatConfigPtr config)
        : TSchemalessFormatWriterBase(
            nameTable,
            output,
            enableContextSaving,
            controlAttributesConfig,
            keyColumnCount)
        , Config_(config)
        , TypeSet_(config)
    {
        auto tableCount = config->FileIndices.size();
        Descriptors_.resize(tableCount);
        for (size_t tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
            auto& descriptor = Descriptors_[tableIndex];

            const auto* messageDescriptor = TypeSet_.GetMessageDescriptor(tableIndex);
            descriptor.MessageDescriptor = messageDescriptor;

            int fieldCount = messageDescriptor->field_count();
            for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
                const auto* fieldDescriptor = messageDescriptor->field(fieldIndex);

                auto columnName = fieldDescriptor->options().GetExtension(column_name);
                if (columnName.empty()) {
                    columnName = fieldDescriptor->options().GetExtension(key_column_name);
                    if (columnName.empty()) {
                        columnName = fieldDescriptor->name();
                    }
                }

                descriptor.NameToField_[columnName] = fieldDescriptor;
            }
        }
    }

private:
    void PrepareFieldInfo(int tableIndex)
    {
        auto& descriptor = Descriptors_[tableIndex];

        if (NameTable_->GetSize() <= descriptor.Fields.size()) {
            return;
        }

        for (int i = descriptor.Fields.size(); i < NameTable_->GetSize(); ++i) {
            auto name = TString(NameTable_->GetName(i));
            if (name.StartsWith(SystemColumnNamePrefix)) {
                descriptor.Fields.push_back(TDescriptor::TField());
                continue;
            }

            auto it = descriptor.NameToField_.find(name);
            if (it == descriptor.NameToField_.end()) {
                descriptor.Fields.push_back(TDescriptor::TField());
                continue;
            }

            const auto* fieldDescriptor = it->second;
            auto number = fieldDescriptor->number();
            auto type = static_cast<WireFormatLite::FieldType>(fieldDescriptor->type());

            descriptor.Fields.push_back(TDescriptor::TField(
                fieldDescriptor,
                WireFormatLite::MakeTag(number, WireFormatLite::WireTypeForFieldType(type)),
                WireFormatLite::TagSize(number, type)));
        }
    }

    void ValidateType(const TUnversionedValue& value, EValueType type)
    {
        if (Y_UNLIKELY(value.Type != type)) {
            THROW_ERROR_EXCEPTION("Invalid protobuf storage type %v, type %v expected",
                value.Type,
                type);
        }
    }

    virtual void DoWrite(const TRange<TUnversionedRow>& rows) override
    {
        auto* stream = GetOutputStream();

        int rowCount = static_cast<int>(rows.Size());
        for (int index = 0; index < rowCount; ++index) {
            auto row = rows[index];

            if (CheckKeySwitch(row, index + 1 == rowCount)) {
                WritePod(*stream, static_cast<ui32>(-2));
            }

            WriteControlAttributes(row);

            PrepareFieldInfo(CurrentTableIndex_);

            const auto& descriptor = Descriptors_[CurrentTableIndex_];

            std::vector<ui32> enumValues;
            size_t enumIndex = 0;

            int size = 0;
            for (const auto& value : row) {
                if (value.Type == EValueType::Null) {
                    continue;
                }
                const auto& field = descriptor.Fields[value.Id];
                if (!field.Descriptor) {
                    continue;
                }

                size += field.TagSize;

                switch (field.Descriptor->type()) {
                    case FieldDescriptor::TYPE_STRING:
                    case FieldDescriptor::TYPE_BYTES:
                        ValidateType(value, EValueType::String);
                        size += WireFormatLite::UInt32Size(value.Length);
                        size += value.Length;
                        break;
                    case FieldDescriptor::TYPE_UINT64:
                        ValidateType(value, EValueType::Uint64);
                        size += WireFormatLite::UInt64Size(value.Data.Uint64);
                        break;
                    case FieldDescriptor::TYPE_UINT32:
                        ValidateType(value, EValueType::Uint64);
                        size += WireFormatLite::UInt32Size(value.Data.Uint64);
                        break;
                    case FieldDescriptor::TYPE_INT64:
                        ValidateType(value, EValueType::Int64);
                        size += WireFormatLite::Int64Size(value.Data.Int64);
                        break;
                    case FieldDescriptor::TYPE_INT32:
                        ValidateType(value, EValueType::Int64);
                        size += WireFormatLite::Int32Size(value.Data.Int64);
                        break;
                    case FieldDescriptor::TYPE_SINT64:
                        ValidateType(value, EValueType::Int64);
                        size += WireFormatLite::SInt64Size(value.Data.Int64);
                        break;
                    case FieldDescriptor::TYPE_SINT32:
                        ValidateType(value, EValueType::Int64);
                        size += WireFormatLite::SInt32Size(value.Data.Int64);
                        break;
                    case FieldDescriptor::TYPE_FIXED64:
                        ValidateType(value, EValueType::Uint64);
                        size += sizeof(ui64);
                        break;
                    case FieldDescriptor::TYPE_FIXED32:
                        ValidateType(value, EValueType::Uint64);
                        size += sizeof(ui32);
                        break;
                    case FieldDescriptor::TYPE_SFIXED64:
                        ValidateType(value, EValueType::Int64);
                        size += sizeof(i64);
                        break;
                    case FieldDescriptor::TYPE_SFIXED32:
                        ValidateType(value, EValueType::Int64);
                        size += sizeof(i32);
                        break;
                    case FieldDescriptor::TYPE_DOUBLE:
                        ValidateType(value, EValueType::Double);
                        size += sizeof(double);
                        break;
                    case FieldDescriptor::TYPE_FLOAT:
                        ValidateType(value, EValueType::Double);
                        size += sizeof(float);
                        break;
                    case FieldDescriptor::TYPE_BOOL:
                        ValidateType(value, EValueType::Boolean);
                        size += 1;
                        break;
                    case FieldDescriptor::TYPE_ENUM:
                        if (value.Type == EValueType::Uint64) {
                            size += WireFormatLite::UInt32Size(value.Data.Uint64);
                        } else if (value.Type == EValueType::String) {
                            auto enumString = TString(value.Data.String, value.Length);
                            auto* enumDescriptor = field.Descriptor->enum_type();
                            auto* enumValueDescriptor = enumDescriptor->FindValueByName(enumString);
                            if (!enumValueDescriptor) {
                                THROW_ERROR_EXCEPTION("Invalid string value %v for %v",
                                    enumString,
                                    field.Descriptor->DebugString());
                            }

                            auto enumValue = enumValueDescriptor->number();
                            enumValues.push_back(enumValue);
                            size += WireFormatLite::UInt32Size(enumValue);
                        } else {
                            THROW_ERROR_EXCEPTION("Protobuf enums can be stored only in uint64 or string column");
                        }
                        break;
                    case FieldDescriptor::TYPE_MESSAGE:
                        if (Config_->NestedMessagesMode == ENestedMessagesMode::Protobuf) {
                            ValidateType(value, EValueType::String);
                            size += WireFormatLite::UInt32Size(value.Length);
                            size += value.Length;
                        } else {
                            THROW_ERROR_EXCEPTION("Only protobuf nested messages are currently supported");
                        }
                        break;
                    default:
                        THROW_ERROR_EXCEPTION("Invalid protobuf field type %v for %v",
                            static_cast<int>(field.Descriptor->type()),
                            field.Descriptor->DebugString());
                }
            }

            WritePod(*stream, static_cast<ui32>(size));

            for (const auto& value : row) {
                if (value.Type == EValueType::Null) {
                    continue;
                }
                const auto& field = descriptor.Fields[value.Id];
                if (!field.Descriptor) {
                    continue;
                }

                WriteVarUint32(stream, field.Tag);

                switch (field.Descriptor->type()) {
                    case FieldDescriptor::TYPE_STRING:
                    case FieldDescriptor::TYPE_BYTES:
                        WriteVarUint32(stream, value.Length);
                        stream->Write(value.Data.String, value.Length);
                        break;
                    case FieldDescriptor::TYPE_UINT64:
                        WriteVarUint64(stream, value.Data.Uint64);
                        break;
                    case FieldDescriptor::TYPE_UINT32:
                        WriteVarUint32(stream, value.Data.Uint64);
                        break;
                    case FieldDescriptor::TYPE_INT64:
                        WriteVarUint64(stream, value.Data.Int64); // no zigzag
                        break;
                    case FieldDescriptor::TYPE_INT32:
                        WriteVarUint32(stream, value.Data.Int64); // no zigzag
                        break;
                    case FieldDescriptor::TYPE_SINT64:
                        WriteVarInt64(stream, value.Data.Int64); // zigzag
                        break;
                    case FieldDescriptor::TYPE_SINT32:
                        WriteVarInt32(stream, value.Data.Int64); // zigzag
                        break;
                    case FieldDescriptor::TYPE_FIXED64:
                        WritePod(*stream, value.Data.Uint64);
                        break;
                    case FieldDescriptor::TYPE_FIXED32:
                        WritePod(*stream, static_cast<ui32>(value.Data.Uint64));
                        break;
                    case FieldDescriptor::TYPE_SFIXED64:
                        WritePod(*stream, value.Data.Int64);
                        break;
                    case FieldDescriptor::TYPE_SFIXED32:
                        WritePod(*stream, static_cast<i32>(value.Data.Int64));
                        break;
                    case FieldDescriptor::TYPE_DOUBLE:
                        WritePod(*stream, value.Data.Double);
                        break;
                    case FieldDescriptor::TYPE_FLOAT:
                        WritePod(*stream, static_cast<float>(value.Data.Double));
                        break;
                    case FieldDescriptor::TYPE_BOOL:
                        WritePod(*stream, static_cast<ui8>(value.Data.Boolean));
                        break;
                    case FieldDescriptor::TYPE_ENUM:
                        if (value.Type == EValueType::Uint64) {
                            WriteVarUint32(stream, value.Data.Uint64);
                        } else if (value.Type == EValueType::String) {
                            WriteVarUint32(stream, enumValues[enumIndex]);
                            ++enumIndex;
                        } else {
                            Y_UNREACHABLE();
                        }
                        break;
                    case FieldDescriptor::TYPE_MESSAGE:
                        if (Config_->NestedMessagesMode == ENestedMessagesMode::Protobuf) {
                            WriteVarUint32(stream, value.Length);
                            stream->Write(value.Data.String, value.Length);
                        } else {
                            Y_UNREACHABLE();
                        }
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
        WritePod(*stream, static_cast<ui32>(-1));
        WritePod(*stream, static_cast<ui32>(tableIndex));
    }

    virtual void WriteRangeIndex(i64 rangeIndex) override
    {
        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(-3));
        WritePod(*stream, static_cast<ui32>(rangeIndex));
    }

    virtual void WriteRowIndex(i64 rowIndex) override
    {
        auto* stream = GetOutputStream();
        WritePod(*stream, static_cast<ui32>(-4));
        WritePod(*stream, static_cast<ui64>(rowIndex));
    }

private:
    const TProtobufFormatConfigPtr Config_;
    const TProtobufTypeSet TypeSet_;

    struct TDescriptor
    {
        struct TField
        {
            const FieldDescriptor* Descriptor;
            ui32 Tag;
            int TagSize;

            TField(
                const FieldDescriptor* descriptor = nullptr,
                ui32 tag = 0,
                int tagSize = 0)
                : Descriptor(descriptor)
                , Tag(tag)
                , TagSize(tagSize)
            { }
        };
        std::vector<TField> Fields;
        yhash<TString, const FieldDescriptor*> NameToField_;
        const Descriptor* MessageDescriptor;
    };
    std::vector<TDescriptor> Descriptors_;

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
    return New<TSchemalessWriterForProtobuf>(
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount,
        config);
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
