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
#include <util/string/escape.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT {
namespace NFormats {

using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::internal::WireFormatLite;


namespace {

////////////////////////////////////////////////////////////////////////////////

class TRowParser
{
public:
    explicit TRowParser(TStringBuf strbuf)
        : Begin_(strbuf.Data())
        , End_(strbuf.Data() + strbuf.Size())
        , Current_(strbuf.Data())
    { }

    ui32 ReadVarUint32()
    {
        ui32 result;
        Current_ += ::NYT::ReadVarUint32(Current_, End_, &result);
        return result;
    }

    ui64 ReadVarUint64()
    {
        ui64 result;
        Current_ += ::NYT::ReadVarUint64(Current_, End_, &result);
        return result;
    }

    i64 ReadVarSint64()
    {
        i64 value;
        Current_ += ReadVarInt64(Current_, End_, &value);
        return value;
    }

    i32 ReadVarSint32()
    {
        i32 value;
        Current_ += ReadVarInt32(Current_, End_, &value);
        return value;
    }

    template <typename T>
    T ReadFixed()
    {
        if (Current_ + sizeof(T) > End_) {
            THROW_ERROR_EXCEPTION("Cannot read value of %v bytes, message exhausted",
                sizeof(T));
        }
        T result = *reinterpret_cast<const T*>(Current_);
        Current_ += sizeof(T);
        return result;
    }

    TStringBuf ReadLengthDelimited()
    {
        ui32 length = ReadVarUint32();

        ValidateLength(length);

        auto result = TStringBuf(Current_, length);
        Current_ += length;
        return result;
    }

    bool IsExhausted() const
    {
        return Current_ >= End_;
    }

    std::vector<TErrorAttribute> GetContextErrorAttributes() const
    {
        constexpr int contextMargin = 50;

        auto contextBegin = Begin_ + contextMargin > Current_ ? Begin_ : Current_ - contextMargin;
        auto contextEnd = Current_ + contextMargin > End_ ? End_ : Current_ + contextMargin;
        size_t contextPos = Current_ - contextBegin;

        TString contextString;
        return {
            TErrorAttribute("context", EscapeC(TStringBuf(contextBegin, contextEnd), contextString)),
            TErrorAttribute("context_pos", contextPos)
        };
    }

private:
    void ValidateLength(ui32 length) const
    {
        if (Current_ + length > End_) {
            THROW_ERROR_EXCEPTION("Broken protobuf message: field with length %v is out of message bounds",
                length)
                << GetContextErrorAttributes();
        }
    }

private:
    const char* Begin_;
    const char* End_;
    const char* Current_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

static constexpr ui32 InvalidColumnId = -1;

////////////////////////////////////////////////////////////////////////////////

static WireFormatLite::WireType GetWireTypeForProtobufType(EProtobufType type)
{
    switch (type) {
        case EProtobufType::Double:
            return WireFormatLite::WIRETYPE_FIXED64;
        case EProtobufType::Float:
            return WireFormatLite::WIRETYPE_FIXED32;

        case EProtobufType::Int64:
        case EProtobufType::Uint64:
        case EProtobufType::Sint64:
            return WireFormatLite::WIRETYPE_VARINT;
        case EProtobufType::Fixed64:
        case EProtobufType::Sfixed64:
            return WireFormatLite::WIRETYPE_FIXED64;

        case EProtobufType::Int32:
        case EProtobufType::Uint32:
        case EProtobufType::Sint32:
            return WireFormatLite::WIRETYPE_VARINT;
        case EProtobufType::Fixed32:
        case EProtobufType::Sfixed32:
            return WireFormatLite::WIRETYPE_FIXED32;

        case EProtobufType::Bool:
            return WireFormatLite::WIRETYPE_VARINT;
        case EProtobufType::String:
        case EProtobufType::Bytes:
            return WireFormatLite::WIRETYPE_LENGTH_DELIMITED;

        case EProtobufType::EnumInt:
        case EProtobufType::EnumString:
            return WireFormatLite::WIRETYPE_VARINT;

        case EProtobufType::Message:
            return WireFormatLite::WIRETYPE_LENGTH_DELIMITED;
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProtobufParserState,
    (InsideLength)
    (InsideData)
);

////////////////////////////////////////////////////////////////////////////////

struct TFieldInfo
{
    EProtobufType ProtobufType;
    WireFormatLite::WireType ExpectedWireType;
    ui32 ColumnId = InvalidColumnId;
    const TEnumerationDescription* EnumerationDescription;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufParser
    : public IParser
{
public:
    using EState = EProtobufParserState;

    TProtobufParser(
        IValueConsumer* valueConsumer,
        TProtobufFormatDescriptionPtr description,
        ui32 tableIndex)
        : ValueConsumer_(valueConsumer)
        , Description_(description)
        , Fields_(MinHashTag_)
        , TableIndex_(tableIndex)
    {
        auto nameTable = ValueConsumer_->GetNameTable();

        const auto& tableDescription = Description_->GetTableDescription(tableIndex);

        for (const auto& pair : tableDescription.Columns) {
            const auto& column = pair.second;
            auto& field = GetField(column.GetFieldNumber());
            field.ProtobufType = column.Type;
            field.ExpectedWireType = GetWireTypeForProtobufType(column.Type);
            field.ColumnId = nameTable->GetIdOrRegisterName(column.Name);
            field.EnumerationDescription = column.EnumerationDescription;
        }
    }

    virtual void Read(TStringBuf data) override
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
    TFieldInfo& GetField(ui32 tag)
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
        }
        Y_UNREACHABLE();
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

    void OutputRow(TStringBuf value)
    {
        TRowParser rowParser(value);

        ValueConsumer_->OnBeginRow();

        while (!rowParser.IsExhausted()) {
            ui32 wireTag = rowParser.ReadVarUint32();

            auto fieldNumber = WireFormatLite::GetTagFieldNumber(wireTag);

            const auto& field = GetField(fieldNumber);
            auto columnId = field.ColumnId;
            if (columnId == InvalidColumnId ||
                WireFormatLite::GetTagWireType(wireTag) != field.ExpectedWireType)
            {
                THROW_ERROR_EXCEPTION("Unexpected wire tag")
                    << TErrorAttribute("field_number", fieldNumber)
                    << TErrorAttribute("table_index", TableIndex_)
                    << rowParser.GetContextErrorAttributes();
            }

            switch (field.ProtobufType) {
                case EProtobufType::String:
                case EProtobufType::Message:
                case EProtobufType::Bytes: {
                    auto value = rowParser.ReadLengthDelimited();
                    ValueConsumer_->OnValue(MakeUnversionedStringValue(value, columnId));
                    break;
                }
                case EProtobufType::Uint64: {
                    auto value = rowParser.ReadVarUint64();
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    break;
                }
                case EProtobufType::Uint32: {
                    auto value = rowParser.ReadVarUint32();
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    break;
                }
                case EProtobufType::Int64: {
                    // Value is *not* zigzag encoded, so we use Uint64 intentionally.
                    auto value = rowParser.ReadVarUint64();
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case EProtobufType::EnumInt:
                case EProtobufType::Int32: {
                    // Value is *not* zigzag encoded, so we use Uint64 intentionally.
                    auto value = rowParser.ReadVarUint64();
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case EProtobufType::Sint64: {
                    auto value = rowParser.ReadVarSint64();
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case EProtobufType::Sint32: {
                    auto value = rowParser.ReadVarSint32();
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case EProtobufType::Fixed64: {
                    auto value = rowParser.ReadFixed<ui64>();
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    break;
                }
                case EProtobufType::Fixed32: {
                    auto value = rowParser.ReadFixed<ui32>();
                    ValueConsumer_->OnValue(MakeUnversionedUint64Value(value, columnId));
                    break;
                }
                case EProtobufType::Sfixed64: {
                    auto value = rowParser.ReadFixed<i64>();
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case EProtobufType::Sfixed32: {
                    auto value = rowParser.ReadFixed<i32>();
                    ValueConsumer_->OnValue(MakeUnversionedInt64Value(value, columnId));
                    break;
                }
                case EProtobufType::Double: {
                    auto value = rowParser.ReadFixed<double>();
                    ValueConsumer_->OnValue(MakeUnversionedDoubleValue(value, columnId));
                    break;
                }
                case EProtobufType::Float: {
                    auto value = rowParser.ReadFixed<float>();
                    ValueConsumer_->OnValue(MakeUnversionedDoubleValue(value, columnId));
                    break;
                }
                case EProtobufType::Bool: {
                    auto value = rowParser.ReadVarUint64();
                    ValueConsumer_->OnValue(MakeUnversionedBooleanValue(value, columnId));
                    break;
                }
                case EProtobufType::EnumString: {
                    i64 value = rowParser.ReadVarUint64();
                    YCHECK(field.EnumerationDescription);
                    const auto& enumString = field.EnumerationDescription->GetValueName(value);

                    ValueConsumer_->OnValue(MakeUnversionedStringValue(enumString, columnId));
                    break;
                }
                default:
                    THROW_ERROR_EXCEPTION("Field has invalid type %Qv",
                        field.ProtobufType)
                    << TErrorAttribute("field_number", fieldNumber)
                    << TErrorAttribute("table_index", TableIndex_)
                    << rowParser.GetContextErrorAttributes();
            }
        }

        ValueConsumer_->OnEndRow();
    }

private:
    IValueConsumer* const ValueConsumer_;

    TProtobufFormatDescriptionPtr Description_;

    std::vector<TFieldInfo> Fields_;
    THashMap<ui32, TFieldInfo> HashFields_;
    const ui32 TableIndex_;
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
    auto formatDescription = New<TProtobufFormatDescription>();
    formatDescription->Init(config);
    return std::unique_ptr<IParser>(
        new TProtobufParser(
            consumer,
            formatDescription,
            tableIndex));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

