#include "protobuf_parser.h"

#include "protobuf.h"
#include "parser.h"
#include "yson_map_to_unversioned_value.h"

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/table_consumer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/table_client/value_consumer.h>

#include <yt/core/misc/varint.h>

#include <util/generic/buffer.h>
#include <util/string/escape.h>

#include <contrib/libs/protobuf/wire_format_lite_inl.h>

namespace NYT::NFormats {

using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::internal::WireFormatLite;


namespace {

////////////////////////////////////////////////////////////////////////////////

class TProtobufParserFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    TProtobufParserFieldDescription() = default;

    explicit TProtobufParserFieldDescription(TProtobufFieldDescription description)
        : TProtobufFieldDescriptionBase(std::move(description))
        , Children_(std::move_iterator(description.Children.begin()), std::move_iterator(description.Children.end()))
    {
        // Setup field number to child index mapping.
        int vectorSize = 0;
        for (const auto& child : Children_) {
            if (child.GetFieldNumber() < MaxFieldNumberVectorSize && vectorSize <= child.GetFieldNumber()) {
                vectorSize = child.GetFieldNumber() + 1;
            }
        }

        FieldNumberToChildIndexVector_.assign(vectorSize, InvalidChildIndex);
        FieldNumberToChildIndexMap_.clear();

        for (int childIndex = 0; childIndex < static_cast<int>(Children_.size()); ++childIndex) {
            auto fieldNumber = Children_[childIndex].GetFieldNumber();
            if (fieldNumber < vectorSize) {
                FieldNumberToChildIndexVector_[fieldNumber] = childIndex;
            } else {
                FieldNumberToChildIndexMap_.emplace(fieldNumber, childIndex);
            }
        }
    }

    int FieldNumberToChildIndex(int fieldNumber) const
    {
        int index;
        if (fieldNumber < FieldNumberToChildIndexVector_.size()) {
            index = FieldNumberToChildIndexVector_[fieldNumber];
            if (Y_UNLIKELY(index == InvalidChildIndex)) {
                THROW_ERROR_EXCEPTION("Unexpected field number %v", fieldNumber);
            }
        } else {
            auto it = FieldNumberToChildIndexMap_.find(fieldNumber);
            if (Y_UNLIKELY(it == FieldNumberToChildIndexMap_.end())) {
                THROW_ERROR_EXCEPTION("Unexpected field number %v", fieldNumber);
            }
            index = it->second;
        }
        return index;
    }

    const std::vector<TProtobufParserFieldDescription>& GetChildren() const
    {
        return Children_;
    }

private:
    static constexpr int InvalidChildIndex = -1;
    static constexpr int MaxFieldNumberVectorSize = 256;

    std::vector<TProtobufParserFieldDescription> Children_;
    std::vector<int> FieldNumberToChildIndexVector_;
    THashMap<int, int> FieldNumberToChildIndexMap_;
};

////////////////////////////////////////////////////////////////////////////////

class TRowParser
{
public:
    explicit TRowParser(TStringBuf strbuf)
        : Begin_(strbuf.data())
        , End_(strbuf.data() + strbuf.size())
        , Current_(strbuf.data())
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

//
// This struct can represent either
//  1) a real column; |ChildIndex| means index in protobuf config; OR
//  2) protobuf binary representation (inside EValueType::String)
//     for a structured message field;
//     in this case |ChildIndex| means the index of a field inside the message; OR
//  3) unversioned-value or protobuf representation for a repeated field;
//     meaning of |ChildIndex| is as in (1) or (2).
struct TField
{
    TUnversionedValue Value;
    int ChildIndex;
};

class TCountingSorter
{
public:
    // Sort a vector of |TField| by |ChildIndex| using counting sort.
    // |ChildIndex| must be in range [0, |rangeSize|).
    void Sort(std::vector<TField>* elements, int rangeSize)
    {
        if (elements->size() <= 1) {
            return;
        }
        Counts_.assign(rangeSize, 0);
        for (const auto& element : *elements) {
            ++Counts_[element.ChildIndex];
        }
        for (int i = 1; i < static_cast<int>(Counts_.size()); ++i) {
            Counts_[i] += Counts_[i - 1];
        }
        Result_.resize(elements->size());
        for (auto it = elements->rbegin(); it != elements->rend(); ++it) {
            auto childIndex = it->ChildIndex;
            Result_[Counts_[childIndex] - 1] = std::move(*it);
            --Counts_[childIndex];
        }
        std::swap(Result_, *elements);
    }

private:
    std::vector<TField> Result_;
    std::vector<int> Counts_;
};

////////////////////////////////////////////////////////////////////////////////

int ComputeDepth(const TProtobufParserFieldDescription& description)
{
    int depth = 0;
    for (const auto& child : description.GetChildren()) {
        depth = std::max(depth, ComputeDepth(child) + 1);
    }
    return depth;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProtobufParserState,
    (InsideLength)
    (InsideData)
);

////////////////////////////////////////////////////////////////////////////////

class TProtobufParser
    : public IParser
{
public:
    using EState = EProtobufParserState;

    // NB(levysotsky): We expect the description to have only one table,
    // the |tableIndex| parameter is for debugging purposes only.
    TProtobufParser(
        IValueConsumer* valueConsumer,
        TProtobufFormatDescriptionPtr description,
        int tableIndex,
        EComplexTypeMode complexTypeMode)
        : ValueConsumer_(valueConsumer)
          , Description_(std::move(description))
          , TableIndex_(tableIndex)
        // NB. We use ColumnConsumer_ to generate yson representation of complex types we don't want additional
        // conversions so we use Positional mode.
        // At the same time we use OtherColumnsConsumer_ to feed yson passed by users.
        // This YSON should be in format specified on the format config.
          , ColumnConsumer_(EComplexTypeMode::Positional, valueConsumer)
          , OtherColumnsConsumer_(complexTypeMode, valueConsumer)
    {
        YT_VERIFY(Description_->GetTableCount() == 1);
        const auto& columns = Description_->GetTableDescription(0).Columns;

        auto nameTable = ValueConsumer_->GetNameTable();

        TProtobufFieldDescription rootDescription;
        for (const auto&[name, columnDescription] : columns) {
            rootDescription.Children.push_back(columnDescription);
            ChildColumnIds_.push_back(static_cast<ui16>(nameTable->GetIdOrRegisterName(columnDescription.Name)));
        }

        rootDescription.Type = EProtobufType::StructuredMessage;
        rootDescription.StructElementCount = static_cast<int>(columns.size());
        RootDescription_ = TProtobufParserFieldDescription(std::move(rootDescription));

        FieldVectors_.resize(ComputeDepth(RootDescription_) + 1);
    }

    void Read(TStringBuf data) override
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
    const char* Consume(const char* begin, const char* end)
    {
        switch (State_) {
            case EState::InsideLength:
                return ConsumeLength(begin, end);
            case EState::InsideData:
                return ConsumeData(begin, end);
        }
        YT_ABORT();
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

    void OutputRow(TStringBuf buffer)
    {
        ValueConsumer_->OnBeginRow();
        ProcessStructuredMessage(buffer, RootDescription_, /* depth */ 0);
        ValueConsumer_->OnEndRow();
    }

    void ProcessStructuredMessage(TStringBuf buffer, const TProtobufParserFieldDescription& description, int depth)
    {
        auto& fields = FieldVectors_[depth];
        fields.clear();

        TRowParser rowParser(buffer);
        try {
            while (!rowParser.IsExhausted()) {
                ui32 wireTag = rowParser.ReadVarUint32();
                auto fieldNumber = WireFormatLite::GetTagFieldNumber(wireTag);
                int childIndex = description.FieldNumberToChildIndex(fieldNumber);
                const auto& childDescription = description.GetChildren()[childIndex];

                if (Y_UNLIKELY(wireTag != childDescription.WireTag)) {
                    THROW_ERROR_EXCEPTION("Expected wire tag for field %Qv to be %v, got %v",
                        childDescription.Name,
                        childDescription.WireTag,
                        wireTag)
                        << TErrorAttribute("field_number", fieldNumber)
                        << TErrorAttribute("table_index", TableIndex_);
                }

                if (childDescription.Packed) {
                    auto elementsParser = TRowParser(rowParser.ReadLengthDelimited());
                    while (!elementsParser.IsExhausted()) {
                        ReadAndProcessUnversionedValue(elementsParser, childIndex, childDescription, depth, &fields);
                    }
                } else {
                    ReadAndProcessUnversionedValue(rowParser, childIndex, childDescription, depth, &fields);
                }
            }
        } catch (const TErrorException& exception) {
            THROW_ERROR_EXCEPTION(exception) << rowParser.GetContextErrorAttributes();
        }

        CountingSorter_.Sort(&fields, static_cast<int>(description.GetChildren().size()));

        const auto inRoot = (depth == 0);

        int structElementIndex = 0;
        auto skipElements = [&](int targetIndex) {
            if (inRoot) {
                return;
            }
            YT_VERIFY(structElementIndex <= targetIndex);
            while (structElementIndex < targetIndex) {
                ColumnConsumer_.OnEntity();
                ++structElementIndex;
            }
        };

        auto fieldIt = fields.begin();
        auto childrenCount = static_cast<int>(description.GetChildren().size());
        for (int childIndex = 0; childIndex < childrenCount; ++childIndex) {
            const auto& childDescription = description.GetChildren()[childIndex];
            skipElements(childDescription.StructElementIndex);
            if (inRoot) {
                ColumnConsumer_.SetColumnIndex(ChildColumnIds_[childIndex]);
            }
            if (childDescription.Repeated) {
                ColumnConsumer_.OnBeginList();
                while (fieldIt != fields.end() && fieldIt->ChildIndex == childIndex) {
                    ColumnConsumer_.OnListItem();
                    OutputValue(fieldIt->Value, childDescription, depth);
                    ++fieldIt;
                }
                ColumnConsumer_.OnEndList();
            } else {
                bool haveFields = (fieldIt != fields.end() && fieldIt->ChildIndex == childIndex);
                if (haveFields) {
                    OutputValue(fieldIt->Value, childDescription, depth);
                    ++fieldIt;
                } else if (!inRoot) {
                    ColumnConsumer_.OnEntity();
                }
            }
            ++structElementIndex;
        }
        skipElements(description.StructElementCount);
    }

    Y_FORCE_INLINE void OutputValue(
        TUnversionedValue value,
        const TProtobufParserFieldDescription& description,
        int depth)
    {
        switch (description.Type) {
            case EProtobufType::StructuredMessage:
                YT_VERIFY(value.Type == EValueType::String);
                ColumnConsumer_.OnBeginList();
                ProcessStructuredMessage(TStringBuf(value.Data.String, value.Length), description, depth + 1);
                ColumnConsumer_.OnEndList();
                break;
            case EProtobufType::OtherColumns:
                NTableClient::UnversionedValueToYson(value, &OtherColumnsConsumer_);
                break;
            default:
                NTableClient::UnversionedValueToYson(value, &ColumnConsumer_);
                break;
        }
    }

    // Reads unversioned value depending on the field type.
    // If |depth == 0| and the field is not repeated we process it according to type.
    // Otherwise, we append it to |fields| vector.
    Y_FORCE_INLINE void ReadAndProcessUnversionedValue(
        TRowParser& rowParser,
        int childIndex,
        const TProtobufParserFieldDescription& description,
        int depth,
        std::vector<TField>* fields)
    {
        const auto inRoot = (depth == 0);
        const auto addToFields = (!inRoot || description.Repeated);
        const auto id = (depth == 0) ? ChildColumnIds_[childIndex] : static_cast<ui16>(0);
        TUnversionedValue value;
        switch (description.Type) {
            case EProtobufType::StructuredMessage:
                value = MakeUnversionedStringValue(rowParser.ReadLengthDelimited(), id);
                if (!addToFields) {
                    if (inRoot) {
                        ColumnConsumer_.SetColumnIndex(id);
                    }
                    ColumnConsumer_.OnBeginList();
                    ProcessStructuredMessage(TStringBuf(value.Data.String, value.Length), description, depth + 1);
                    ColumnConsumer_.OnEndList();
                    return;
                }
                break;
            case EProtobufType::OtherColumns:
                value = MakeUnversionedAnyValue(rowParser.ReadLengthDelimited(), id);
                if (!addToFields) {
                    NTableClient::UnversionedValueToYson(value, &OtherColumnsConsumer_);
                    return;
                }
                break;
            case EProtobufType::Any:
                value = MakeUnversionedAnyValue(rowParser.ReadLengthDelimited(), id);
                if (!addToFields) {
                    if (inRoot) {
                        ColumnConsumer_.SetColumnIndex(id);
                    }
                    NTableClient::UnversionedValueToYson(value, &ColumnConsumer_);
                    return;
                }
                break;
            case EProtobufType::String:
            case EProtobufType::Message:
            case EProtobufType::Bytes:
                value = MakeUnversionedStringValue(rowParser.ReadLengthDelimited(), id);
                break;
            case EProtobufType::Uint64:
                value = MakeUnversionedUint64Value(rowParser.ReadVarUint64(), id);
                break;
            case EProtobufType::Uint32:
                value = MakeUnversionedUint64Value(rowParser.ReadVarUint32(), id);
                break;
            case EProtobufType::Int64:
                // Value is *not* zigzag encoded, so we use Uint64 intentionally.
                value = MakeUnversionedInt64Value(rowParser.ReadVarUint64(), id);
                break;
            case EProtobufType::EnumInt:
            case EProtobufType::Int32:
                // Value is *not* zigzag encoded, so we use Uint64 intentionally.
                value = MakeUnversionedInt64Value(rowParser.ReadVarUint64(), id);
                break;
            case EProtobufType::Sint64:
                value = MakeUnversionedInt64Value(rowParser.ReadVarSint64(), id);
                break;
            case EProtobufType::Sint32:
                value = MakeUnversionedInt64Value(rowParser.ReadVarSint32(), id);
                break;
            case EProtobufType::Fixed64:
                value = MakeUnversionedUint64Value(rowParser.ReadFixed<ui64>(), id);
                break;
            case EProtobufType::Fixed32:
                value = MakeUnversionedUint64Value(rowParser.ReadFixed<ui32>(), id);
                break;
            case EProtobufType::Sfixed64:
                value = MakeUnversionedInt64Value(rowParser.ReadFixed<i64>(), id);
                break;
            case EProtobufType::Sfixed32:
                value = MakeUnversionedInt64Value(rowParser.ReadFixed<i32>(), id);
                break;
            case EProtobufType::Double:
                value = MakeUnversionedDoubleValue(rowParser.ReadFixed<double>(), id);
                break;
            case EProtobufType::Float:
                value = MakeUnversionedDoubleValue(rowParser.ReadFixed<float>(), id);
                break;
            case EProtobufType::Bool:
                value = MakeUnversionedBooleanValue(rowParser.ReadVarUint64(), id);
                break;
            case EProtobufType::EnumString: {
                auto enumValue = static_cast<i32>(rowParser.ReadVarUint64());
                YT_VERIFY(description.EnumerationDescription);
                const auto& enumString = description.EnumerationDescription->GetValueName(enumValue);
                value = MakeUnversionedStringValue(enumString, id);
                break;
            }
            default: {
                auto fieldNumber = WireFormatLite::GetTagFieldNumber(static_cast<ui32>(description.WireTag));
                THROW_ERROR_EXCEPTION("Field has invalid type %Qlv",
                    description.Type)
                    << TErrorAttribute("field_number", fieldNumber)
                    << TErrorAttribute("table_index", TableIndex_)
                    << rowParser.GetContextErrorAttributes();
            }
        }
        if (addToFields) {
            fields->push_back({value, childIndex});
        } else {
            ValueConsumer_->OnValue(value);
        }
    }

private:
    IValueConsumer* const ValueConsumer_;

    TProtobufFormatDescriptionPtr Description_;
    int TableIndex_;

    TProtobufParserFieldDescription RootDescription_;
    std::vector<ui16> ChildColumnIds_;

    TYsonToUnversionedValueConverter ColumnConsumer_;
    TYsonMapToUnversionedValueConverter OtherColumnsConsumer_;

    std::vector<std::vector<TField>> FieldVectors_;
    TCountingSorter CountingSorter_;

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
    bool newFormat = !config->Tables.empty();
    if (newFormat) {
        // Retain only one table config, as we have only one schema here.
        config = NYTree::CloneYsonSerializable(config);
        if (tableIndex >= config->Tables.size()) {
            THROW_ERROR_EXCEPTION("Protobuf format does not have table with index %v",
                tableIndex);
        }
        config->Tables = {config->Tables[tableIndex]};
    }
    formatDescription->Init(
        config,
        {consumer->GetSchema()},
        /* validateMissingFieldsOptionality */ true);
    return std::make_unique<TProtobufParser>(
        consumer,
        formatDescription,
        tableIndex,
        config->ComplexTypeMode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

