#include "protobuf_parser.h"

#include "protobuf.h"
#include "parser.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/value_consumer.h>
#include <yt/ytlib/table_client/name_table.h>

#include <yt/core/misc/varint.h>

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <util/generic/buffer.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT {
namespace NFormats {

using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProtobufParserState,
    (InsideLength)
    (InsideData)
);

class TProtobufParser
    : public IParser
{
public:
    using EState = EProtobufParserState;

    TProtobufParser(
        IValueConsumer* valueConsumer,
        TProtobufFormatConfigPtr config,
        int tableIndex)
        : ValueConsumer_(valueConsumer)
        , Config_(config)
        , TypeSet_(config)
        , MessageDescriptor_(TypeSet_.GetMessageDescriptor(tableIndex))
        , Fields_(MinHashTag_)
    {
        auto nameTable = ValueConsumer_->GetNameTable();

        int fieldCount = MessageDescriptor_->field_count();
        for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex) {
            const auto* fieldDescriptor = MessageDescriptor_->field(fieldIndex);

            auto number = fieldDescriptor->number();
            auto type = static_cast<WireFormatLite::FieldType>(fieldDescriptor->type());

            auto tag = WireFormatLite::MakeTag(
                number,
                WireFormatLite::WireTypeForFieldType(type));

            auto& field = GetField(tag);
            field.Descriptor = fieldDescriptor;

            auto columnName = fieldDescriptor->options().GetExtension(column_name);
            if (columnName.empty()) {
                columnName = fieldDescriptor->options().GetExtension(key_column_name);
                if (columnName.empty()) {
                    columnName = fieldDescriptor->name();
                }
            }

            field.ColumnId = nameTable->GetIdOrRegisterName(columnName);
        }
    }

    virtual void Read(const TStringBuf& data) override
    {
        auto current = data.begin();
        while (current != data.end()) {
            current = Consume(current, data.end());
        }
    }

    virtual void Finish() override
    {
        if (State_ != EState::InsideLength || ExpectedBytes_ != sizeof(ui32)) {
            THROW_ERROR_EXCEPTION("Unexpected end of stream");
        }
    }

private:
    struct TField
    {
        const FieldDescriptor* Descriptor = nullptr;
        int ColumnId;
    };

    TField& GetField(ui32 tag)
    {
        return tag < MinHashTag_ ? Fields_[tag] : HashFields_[tag];
    }

    const char* Consume(const char* begin, const char* end)
    {
        switch (State_) {
            case EState::InsideLength:
                return ConsumeLength(begin, end);
            case EState::InsideData:
                return ConsumeData(begin, end);
            default:
                Y_UNREACHABLE();
        }
    }

    const char* ConsumeInt32(const char* begin, const char* end)
    {
        const char* current = begin;
        for (; ExpectedBytes_ != 0 && current != end; ++current, --ExpectedBytes_) {
            Length_.Bytes[sizeof(ui32) - ExpectedBytes_] = *current;
        }
        return current;
    }

    const char* ConsumeLength(const char* begin, const char* end)
    {
        const char* next = ConsumeInt32(begin, end);
        if (ExpectedBytes_ != 0) {
            return next;
        }

        State_ = EState::InsideData;
        ExpectedBytes_ = Length_.Value;

        return ConsumeData(next, end);
    }

    const char* ConsumeData(const char* begin, const char* end)
    {
        const char* current = begin + ExpectedBytes_;
        if (current > end) {
            Data_.append(begin, end);
            ExpectedBytes_ -= (end - begin);
            return end;
        }

        if (Data_.empty()) {
            OutputRow(TStringBuf(begin, current));
        } else {
            Data_.append(begin, current);
            OutputRow(Data_);
        }

        State_ = EState::InsideLength;
        ExpectedBytes_ = sizeof(ui32);
        Data_.clear();
        return current;
    }

    void OutputRow(const TStringBuf& value)
    {
        const char* current = value.Data();
        const char* end = current + value.Size();

        ValueConsumer_->OnBeginRow();

        while (current != end) {
            ui32 tag;
            current += ReadVarUint32(current, &tag);

            auto& field = GetField(tag);
            auto columnId = field.ColumnId;

            switch (field.Descriptor->type()) {
                case FieldDescriptor::TYPE_STRING:
                case FieldDescriptor::TYPE_BYTES: {
                    ui32 length;
                    current += ReadVarUint32(current, &length);
                    ValueConsumer_->OnValue(MakeUnversionedStringValue(
                        TStringBuf(current, length),
                        columnId));
                    current += length;
                    break;
                }
                case FieldDescriptor::TYPE_UINT64: {
                    ui64 value;
                    current += ReadVarUint64(current, &value);
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    break;
                }
                case FieldDescriptor::TYPE_UINT32: {
                    ui32 value;
                    current += ReadVarUint32(current, &value);
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    break;
                }
                case FieldDescriptor::TYPE_INT64: {
                    ui64 value;
                    current += ReadVarUint64(current, &value); // no zigzag
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case FieldDescriptor::TYPE_INT32: {
                    ui32 value;
                    current += ReadVarUint32(current, &value); // no zigzag
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case FieldDescriptor::TYPE_SINT64: {
                    i64 value;
                    current += ReadVarInt64(current, &value); // zigzag
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case FieldDescriptor::TYPE_SINT32: {
                    i32 value;
                    current += ReadVarInt32(current, &value); // zigzag
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case FieldDescriptor::TYPE_FIXED64: {
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(
                        *reinterpret_cast<const ui64*>(current),
                        columnId));
                    current += sizeof(ui64);
                    break;
                }
                case FieldDescriptor::TYPE_FIXED32: {
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(
                        *reinterpret_cast<const ui32*>(current),
                        columnId));
                    current += sizeof(ui32);
                    break;
                }
                case FieldDescriptor::TYPE_SFIXED64: {
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(
                        *reinterpret_cast<const i64*>(current),
                        columnId));
                    current += sizeof(i64);
                    break;
                }
                case FieldDescriptor::TYPE_SFIXED32: {
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(
                        *reinterpret_cast<const i32*>(current),
                        columnId));
                    current += sizeof(i32);
                    break;
                }
                case FieldDescriptor::TYPE_DOUBLE: {
                    ValueConsumer_->OnValue(MakeUnversionedDoubleValue(
                        *reinterpret_cast<const double*>(current),
                        columnId));
                    current += sizeof(double);
                    break;
                }
                case FieldDescriptor::TYPE_FLOAT: {
                    ValueConsumer_->OnValue(MakeUnversionedDoubleValue(
                        *reinterpret_cast<const float*>(current),
                        columnId));
                    current += sizeof(float);
                    break;
                }
                case FieldDescriptor::TYPE_BOOL: {
                    ValueConsumer_->OnValue(MakeUnversionedBooleanValue(
                        static_cast<bool>(*current),
                        columnId));
                    ++current;
                    break;
                }
                case FieldDescriptor::TYPE_ENUM: {
                    ui32 value;
                    current += ReadVarUint32(current, &value);
                    if (Config_->EnumsAsStrings) {
                        auto* enumDescriptor = field.Descriptor->enum_type();
                        auto* enumValueDescriptor = enumDescriptor->FindValueByNumber(value);
                        if (!enumValueDescriptor) {
                            THROW_ERROR_EXCEPTION("Invalid value %v for enum %v",
                                value,
                                field.Descriptor->full_name());
                        }

                        ValueConsumer_->OnValue(MakeUnversionedStringValue(
                            enumValueDescriptor->name(),
                            columnId));
                    } else {
                        ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    }
                    break;
                }
                case FieldDescriptor::TYPE_MESSAGE: {
                    if (Config_->NestedMessagesMode == ENestedMessagesMode::Protobuf) {
                        ui32 length;
                        current += ReadVarUint32(current, &length);
                        ValueConsumer_->OnValue(MakeUnversionedStringValue(
                            TStringBuf(current, length),
                            columnId));
                        current += length;
                    } else {
                        THROW_ERROR_EXCEPTION("Only protobuf nested messages are currently supported");
                    }
                    break;
                }
                default:
                    THROW_ERROR_EXCEPTION("Invalid protobuf field type %v for %v",
                        static_cast<int>(field.Descriptor->type()),
                        field.Descriptor->full_name());
            }
        }

        ValueConsumer_->OnEndRow();
    }

private:
    IValueConsumer* const ValueConsumer_;

    const TProtobufFormatConfigPtr Config_;
    const TProtobufTypeSet TypeSet_;

    const Descriptor* MessageDescriptor_;

    std::vector<TField> Fields_;
    yhash<ui32, TField> HashFields_;
    static constexpr size_t MinHashTag_ = 256;

    EState State_ = EState::InsideLength;
    union
    {
        ui32 Value;
        char Bytes[sizeof(ui32)];
    } Length_;
    ui32 ExpectedBytes_ = sizeof(ui32);

    TString Data_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForProtobuf(
    IValueConsumer* consumer,
    TProtobufFormatConfigPtr config,
    int tableIndex)
{
    return std::unique_ptr<IParser>(
        new TProtobufParser(
            consumer,
            config,
            tableIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

