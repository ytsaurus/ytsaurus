#include "protobuf_parser.h"

#include "protobuf.h"
#include "parser.h"
#include "yson_map_to_unversioned_value.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/misc/varint.h>

#include <util/generic/buffer.h>
#include <util/string/escape.h>

#include <google/protobuf/wire_format_lite.h>

namespace NYT::NFormats {

using namespace NYson;
using namespace NTableClient;

using ::google::protobuf::internal::WireFormatLite;


namespace {

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

    void Skip(WireFormatLite::WireType wireType)
    {
        switch (wireType) {
            case WireFormatLite::WIRETYPE_VARINT:
                ReadVarUint64();
                return;
            case WireFormatLite::WIRETYPE_FIXED64:
                ReadFixed<ui64>();
                return;
            case WireFormatLite::WIRETYPE_LENGTH_DELIMITED:
                ReadLengthDelimited();
                return;
            case WireFormatLite::WIRETYPE_START_GROUP:
            case WireFormatLite::WIRETYPE_END_GROUP:
                THROW_ERROR_EXCEPTION("Unexpected wire type %v", static_cast<int>(wireType));
            case WireFormatLite::WIRETYPE_FIXED32:
                ReadFixed<ui32>();
                return;
        }
        YT_ABORT();
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
    for (const auto& child : description.Children) {
        depth = std::max(depth, ComputeDepth(*child) + 1);
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
        TProtobufParserFormatDescriptionPtr description,
        int tableIndex,
        EComplexTypeMode complexTypeMode)
        : ValueConsumer_(valueConsumer)
        , Description_(std::move(description))
        , TableIndex_(tableIndex)
        , RootChildColumnIds_(Description_->CreateRootChildColumnIds(ValueConsumer_->GetNameTable()))
        , RootChildOutputFlags_(Description_->GetRootDescription().Children.size())
        // NB. We use ColumnConsumer_ to generate yson representation of complex types we don't want additional
        // conversions so we use Positional mode.
        // At the same time we use OtherColumnsConsumer_ to feed yson passed by users.
        // This YSON should be in format specified on the format config.
        , ColumnConsumer_(EComplexTypeMode::Positional, valueConsumer)
        , OtherColumnsConsumer_(complexTypeMode, valueConsumer)
    {
        FieldVectors_.resize(ComputeDepth(Description_->GetRootDescription()) + 1);
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
        const auto& rootDescription = Description_->GetRootDescription();
        RootChildOutputFlags_.assign(rootDescription.Children.size(), false);
        ProcessStructuredMessage(buffer, rootDescription, /* depth */ 0);
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
                auto maybeChildIndex = description.FieldNumberToChildIndex(fieldNumber);
                if (!maybeChildIndex) {
                    rowParser.Skip(WireFormatLite::GetTagWireType(wireTag));
                    continue;
                }
                auto childIndex = *maybeChildIndex;
                const auto& childDescription = *description.Children[childIndex];
                if (Y_UNLIKELY(wireTag != childDescription.WireTag)) {
                    THROW_ERROR_EXCEPTION("Expected wire tag for field %Qv to be %v, got %v",
                        childDescription.GetDebugString(),
                        childDescription.WireTag,
                        wireTag)
                        << TErrorAttribute("field_number", fieldNumber);
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
            THROW_ERROR_EXCEPTION(exception)
                << TErrorAttribute("table_index", TableIndex_)
                << rowParser.GetContextErrorAttributes();
        }

        CountingSorter_.Sort(&fields, static_cast<int>(description.Children.size()));
        OutputChildren(fields, description, depth);
    }

    Y_FORCE_INLINE void OutputChild(
        std::vector<TField>::const_iterator begin,
        std::vector<TField>::const_iterator end,
        const TProtobufParserFieldDescription& childDescription,
        int depth)
    {
        if (childDescription.Repeated) {
            ColumnConsumer_.OnBeginList();
            for (auto it = begin; it != end; ++it) {
                ColumnConsumer_.OnListItem();
                OutputValue(it->Value, childDescription, depth);
            }
            ColumnConsumer_.OnEndList();
        } else {
            if (Y_UNLIKELY(std::distance(begin, end) > 1)) {
                THROW_ERROR_EXCEPTION("Error parsing protobuf: found %v entries for non-repeated field %Qv",
                    std::distance(begin, end),
                    childDescription.GetDebugString())
                    << TErrorAttribute("table_index", TableIndex_);
            }
            OutputValue(begin->Value, childDescription, depth);
        }
    }

    void OutputChildren(
        const std::vector<TField>& fields,
        const TProtobufParserFieldDescription& description,
        int depth)
    {
        const auto inRoot = (depth == 0);

        auto skipElements = [&](int count) {
            if (inRoot) {
                return;
            }
            YT_VERIFY(count >= 0);
            for (int i = 0; i < count; ++i) {
                ColumnConsumer_.OnEntity();
            }
        };

        auto isStructFieldPresentOrLegallyMissing = [&] (int childIndex, int lastOutputStructFieldIndex) {
            const auto& childDescription = *description.Children[childIndex];
            if (ShouldOutputValueImmediately(inRoot, childDescription) && RootChildOutputFlags_[childIndex]) {
                // The value is already output.
                return true;
            }
            if (!childDescription.IsOneofAlternative()) {
                return childDescription.Optional;
            }
            if (childDescription.Parent->Optional) {
                return true;
            }
            if (lastOutputStructFieldIndex == childDescription.StructFieldIndex) {
                // It is not missing.
                return true;
            }
            if (childIndex + 1 == std::ssize(description.Children)) {
                return false;
            }
            const auto& nextChildDescription = *description.Children[childIndex + 1];
            // The current alternative is missing, but the next one corresponds to the same field,
            // so the check is deferred to the next alternative.
            return childDescription.StructFieldIndex == nextChildDescription.StructFieldIndex;
        };

        auto fieldIt = fields.cbegin();
        auto childrenCount = static_cast<int>(description.Children.size());
        auto lastOutputStructFieldIndex = -1;
        for (int childIndex = 0; childIndex < childrenCount; ++childIndex) {
            const auto& childDescription = *description.Children[childIndex];
            auto fieldRangeBegin = fieldIt;
            while (fieldIt != fields.cend() && fieldIt->ChildIndex == childIndex) {
                ++fieldIt;
            }
            if (fieldRangeBegin != fieldIt || (childDescription.Repeated && !childDescription.Optional)) {
                if (Y_UNLIKELY(
                    childDescription.IsOneofAlternative() &&
                    lastOutputStructFieldIndex == childDescription.StructFieldIndex))
                {
                    auto parent = childDescription.Parent;
                    YT_VERIFY(parent);
                    THROW_ERROR_EXCEPTION(
                        "Error parsing protobuf: multiple entries for oneof field %Qv; the second one is %Qv",
                        parent->GetDebugString(),
                        childDescription.GetDebugString())
                        << TErrorAttribute("table_index", TableIndex_);
                }
                skipElements(childDescription.StructFieldIndex - lastOutputStructFieldIndex - 1);
                if (inRoot) {
                    ColumnConsumer_.SetColumnIndex(RootChildColumnIds_[childIndex]);
                    RootChildOutputFlags_[childIndex] = true;
                }
                OutputChild(fieldRangeBegin, fieldIt, childDescription, depth);
                lastOutputStructFieldIndex = childDescription.StructFieldIndex;
            } else {
                if (Y_UNLIKELY(!isStructFieldPresentOrLegallyMissing(childIndex, lastOutputStructFieldIndex))) {
                    const TProtobufParserFieldDescription* actualDescription = &childDescription;
                    if (childDescription.IsOneofAlternative()) {
                        actualDescription = childDescription.Parent;
                        YT_VERIFY(actualDescription);
                    }
                    THROW_ERROR_EXCEPTION("Error parsing protobuf: required field %Qv is missing",
                        actualDescription->GetDebugString());
                }
            }
        }
        skipElements(description.StructFieldCount - lastOutputStructFieldIndex - 1);
    }

    Y_FORCE_INLINE void OutputValue(
        TUnversionedValue value,
        const TProtobufParserFieldDescription& description,
        int depth)
    {
        const auto inRoot = (depth == 0);

        if (description.IsOneofAlternative()) {
            ColumnConsumer_.OnBeginList();
            ColumnConsumer_.OnListItem();
            ColumnConsumer_.OnInt64Scalar(*description.AlternativeIndex);
            ColumnConsumer_.OnListItem();
        }
        switch (description.Type) {
            case EProtobufType::StructuredMessage:
                YT_VERIFY(value.Type == EValueType::String);
                ColumnConsumer_.OnBeginList();
                ProcessStructuredMessage(TStringBuf(value.Data.String, value.Length), description, depth + 1);
                ColumnConsumer_.OnEndList();
                break;
            case EProtobufType::OtherColumns:
                UnversionedValueToYson(value, &OtherColumnsConsumer_);
                break;
            case EProtobufType::Any:
                UnversionedValueToYson(value, &ColumnConsumer_);
                break;
            default:
                if (ShouldOutputValueImmediately(inRoot, description)) {
                    ValueConsumer_->OnValue(value);
                } else {
                    UnversionedValueToYson(value, &ColumnConsumer_);
                }
                break;
        }
        if (description.IsOneofAlternative()) {
            ColumnConsumer_.OnEndList();
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
        const auto id = inRoot ? RootChildColumnIds_[childIndex] : static_cast<ui16>(0);
        auto value = [&] {
            switch (description.Type) {
                case EProtobufType::StructuredMessage:
                    return MakeUnversionedStringValue(rowParser.ReadLengthDelimited(), id);
                case EProtobufType::OtherColumns:
                    return MakeUnversionedAnyValue(rowParser.ReadLengthDelimited(), id);
                case EProtobufType::Any:
                    return MakeUnversionedAnyValue(rowParser.ReadLengthDelimited(), id);
                case EProtobufType::String:
                case EProtobufType::Message:
                case EProtobufType::Bytes:
                    return MakeUnversionedStringValue(rowParser.ReadLengthDelimited(), id);
                case EProtobufType::Uint64:
                    return MakeUnversionedUint64Value(rowParser.ReadVarUint64(), id);
                case EProtobufType::Uint32:
                    return MakeUnversionedUint64Value(rowParser.ReadVarUint32(), id);
                case EProtobufType::Int64:
                    // Value is *not* zigzag encoded, so we use Uint64 intentionally.
                    return MakeUnversionedInt64Value(rowParser.ReadVarUint64(), id);
                case EProtobufType::EnumInt:
                case EProtobufType::Int32:
                    // Value is *not* zigzag encoded, so we use Uint64 intentionally.
                    return MakeUnversionedInt64Value(rowParser.ReadVarUint64(), id);
                case EProtobufType::Sint64:
                    return MakeUnversionedInt64Value(rowParser.ReadVarSint64(), id);
                case EProtobufType::Sint32:
                    return MakeUnversionedInt64Value(rowParser.ReadVarSint32(), id);
                case EProtobufType::Fixed64:
                    return MakeUnversionedUint64Value(rowParser.ReadFixed<ui64>(), id);
                case EProtobufType::Fixed32:
                    return MakeUnversionedUint64Value(rowParser.ReadFixed<ui32>(), id);
                case EProtobufType::Sfixed64:
                    return MakeUnversionedInt64Value(rowParser.ReadFixed<i64>(), id);
                case EProtobufType::Sfixed32:
                    return MakeUnversionedInt64Value(rowParser.ReadFixed<i32>(), id);
                case EProtobufType::Double:
                    return MakeUnversionedDoubleValue(rowParser.ReadFixed<double>(), id);
                case EProtobufType::Float:
                    return MakeUnversionedDoubleValue(rowParser.ReadFixed<float>(), id);
                case EProtobufType::Bool:
                    return MakeUnversionedBooleanValue(rowParser.ReadVarUint64(), id);
                case EProtobufType::EnumString: {
                    auto enumValue = static_cast<i32>(rowParser.ReadVarUint64());
                    YT_VERIFY(description.EnumerationDescription);
                    const auto& enumString = description.EnumerationDescription->GetValueName(enumValue);
                    return MakeUnversionedStringValue(enumString, id);
                }
                case EProtobufType::Oneof:
                    THROW_ERROR_EXCEPTION("Oneof inside oneof is not supported in protobuf format; offending field %Qv",
                        description.GetDebugString())
                        << TErrorAttribute("table_index", TableIndex_);
            }
            YT_ABORT();
        }();
        if (ShouldOutputValueImmediately(inRoot, description)) {
            ColumnConsumer_.SetColumnIndex(id);
            RootChildOutputFlags_[childIndex] = true;
            OutputValue(value, description, depth);
        } else {
            fields->push_back({value, childIndex});
        }
    }

    Y_FORCE_INLINE static bool ShouldOutputValueImmediately(bool inRoot, const TProtobufParserFieldDescription& description)
    {
        return inRoot && !description.Repeated && !description.IsOneofAlternative();
    }

private:
    IValueConsumer* const ValueConsumer_;

    TProtobufParserFormatDescriptionPtr Description_;
    int TableIndex_;

    std::vector<ui16> RootChildColumnIds_;
    std::vector<bool> RootChildOutputFlags_;

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
    bool newFormat = !config->Tables.empty();
    if (newFormat) {
        // Retain only one table config, as we have only one schema here.
        config = NYTree::CloneYsonSerializable(config);
        if (tableIndex >= std::ssize(config->Tables)) {
            THROW_ERROR_EXCEPTION("Protobuf format does not have table with index %v",
                tableIndex);
        }
        config->Tables = {config->Tables[tableIndex]};
    }
    auto formatDescription = New<TProtobufParserFormatDescription>();
    formatDescription->Init(config, {consumer->GetSchema()});
    return std::make_unique<TProtobufParser>(
        consumer,
        formatDescription,
        tableIndex,
        config->ComplexTypeMode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

