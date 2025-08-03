#include "wire_string.h"

#include <yt/yt/orm/library/attributes/proto_visitor.h>

#include <yt/yt/orm/library/mpl/projection.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/helpers.h>

#include <library/cpp/yt/coding/zig_zag.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/string/hex.h>

#include <ranges>

namespace NYT::NOrm::NAttributes {

using google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MapKeyFieldNumber = 1;
static constexpr int MapValueFieldNumber = 2;

////////////////////////////////////////////////////////////////////////////////

template <std::ranges::range TRange>
TWireString FromSerializedRangeImpl(const TRange& serializedRange)
{
    TWireString result;
    for (const auto& serializedProto : serializedRange) {
        result.push_back(TWireStringPart::FromStringView(serializedProto));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

// Internal representation of semi-parsed wire string.
// Used as a parameter for TProtoVisitor.
class TUnpackedWireString
{
public:
    using TFieldNumberToFieldMap = THashMap<int, TWireString>;

    explicit TUnpackedWireString(const NProtoBuf::Descriptor* descriptor)
        : Descriptor_(descriptor)
    { }

    template <std::invocable<int> TExtractValuesFor = NMpl::TConstantProjection<bool, true>>
    TUnpackedWireString(const NProtoBuf::Descriptor* descriptor, TWireString wireString, TExtractValuesFor extractValuesFor = {})
        : Descriptor_(descriptor)
        , WireString_(std::move(wireString))
    {
        YT_VERIFY(Descriptor_);

        auto getPackedFieldType = [&] (int fieldNumber) -> std::optional<WireFormatLite::WireType> {
            if (auto* field = GetDescriptor()->FindFieldByNumber(fieldNumber);
                field && NProtoBuf::FieldDescriptor::IsTypePackable(field->type()))
            {
                return WireFormatLite::WireTypeForFieldType(
                    static_cast<WireFormatLite::FieldType>(field->type()));
            }
            return std::nullopt;
        };

        for (const auto& wireStringPart : WireString_) {
            FillFieldNumberToFieldMap(FieldNumberToField_, wireStringPart, extractValuesFor, getPackedFieldType);
        }
    }

    const TWireString& GetFieldOrEmpty(const NProtoBuf::FieldDescriptor* field) const
    {
        auto iter = FieldNumberToField_.find(field->number());
        if (iter == FieldNumberToField_.end()) {
            return TWireString::Empty;
        }

        return iter->second;
    }

    DEFINE_BYVAL_RO_PROPERTY(const NProtoBuf::Descriptor*, Descriptor);
    DEFINE_BYREF_RO_PROPERTY(TWireString, WireString);
    DEFINE_BYREF_RO_PROPERTY(TFieldNumberToFieldMap, FieldNumberToField);

    template <std::invocable<int> TextractValuesFor, std::invocable<int> TGetPackedFieldType>
    static void FillFieldNumberToFieldMap(
        TFieldNumberToFieldMap& map,
        TWireStringPart wireStringPart,
        TextractValuesFor extractValuesFor = {},
        TGetPackedFieldType getPackedFieldType = {});
};

////////////////////////////////////////////////////////////////////////////////

class TCodedInputStream
    : public google::protobuf::io::CodedInputStream
{
public:
    TCodedInputStream(TWireStringPart wireStringPart)
        : google::protobuf::io::CodedInputStream(wireStringPart.AsSpan().data(), wireStringPart.AsSpan().size())
        , Data_(wireStringPart.AsSpan().data())
    { }

    TWireStringPart Checkpoint()
    {
        auto newCheckpoint = CurrentPosition();
        TWireStringPart result{Data_ + LastCheckpoint_, static_cast<size_t>(newCheckpoint - LastCheckpoint_)};
        LastCheckpoint_ = newCheckpoint;
        return result;
    }

    void CheckpointIf(bool doCheckpoint)
    {
        if (doCheckpoint) {
            Checkpoint();
        }
    }

private:
    const ui8* Data_;
    int LastCheckpoint_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValueType, std::invocable<TCodedInputStream&, TValueType*> TReadNext>
void UnpackRepeatedFieldImpl(TWireString& target, TWireStringPart packedPart, TReadNext readNext)
{
    TCodedInputStream stream(packedPart);
    TValueType buffer;
    while(std::invoke(readNext, stream, &buffer)) {
        target.push_back(stream.Checkpoint());
    }
}

void UnpackRepeatedField(TWireString& target, TWireStringPart packedPart, WireFormatLite::WireType wireType)
{
    switch (wireType) {
        case WireFormatLite::WIRETYPE_VARINT: {
            UnpackRepeatedFieldImpl<ui64>(target, packedPart, &TCodedInputStream::ReadVarint64);
            break;
        }
        case WireFormatLite::WIRETYPE_FIXED32: {
            UnpackRepeatedFieldImpl<ui32>(target, packedPart, &TCodedInputStream::ReadLittleEndian32);
            break;
        }
        case WireFormatLite::WIRETYPE_FIXED64: {
            UnpackRepeatedFieldImpl<ui64>(target, packedPart, &TCodedInputStream::ReadLittleEndian64);
            break;
        }
        default:
            // NB! Only packable types could be unpacked.
            // See (https://protobuf.dev/programming-guides/encoding/#packed) for details.
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <std::invocable<int> TExtractValuesFor, std::invocable<int> TGetPackedFieldType>
void TUnpackedWireString::FillFieldNumberToFieldMap(
    TFieldNumberToFieldMap& map,
    TWireStringPart wireStringPart,
    TExtractValuesFor extractValuesFor,
    TGetPackedFieldType getPackedFieldType)
{
    TCodedInputStream stream(wireStringPart);
    while (ui32 tag = stream.ReadTag()) {
        auto wireType = WireFormatLite::GetTagWireType(tag);
        auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);

        std::optional<WireFormatLite::WireType> packedFieldWireType = getPackedFieldType(fieldNumber);

        switch (wireType) {
            case WireFormatLite::WIRETYPE_VARINT: {
                ui64 dummyVarint;
                stream.CheckpointIf(extractValuesFor(fieldNumber));
                stream.ReadVarint64(&dummyVarint);
                break;
            }
            case WireFormatLite::WIRETYPE_FIXED64: {
                ui64 dummyFixed64;
                stream.CheckpointIf(extractValuesFor(fieldNumber));
                stream.ReadLittleEndian64(&dummyFixed64);
                break;
            }
            case WireFormatLite::WIRETYPE_FIXED32: {
                ui32 dummyFixed32;
                stream.CheckpointIf(extractValuesFor(fieldNumber));
                stream.ReadLittleEndian32(&dummyFixed32);
                break;
            }
            case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                int size;
                stream.ReadVarintSizeAsInt(&size);
                stream.CheckpointIf(extractValuesFor(fieldNumber));
                stream.Skip(size);

                if (packedFieldWireType) {
                    UnpackRepeatedField(map[fieldNumber], stream.Checkpoint(), *packedFieldWireType);
                    continue;
                }
                break;
            }
            default:
                THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::InvalidData,
                    "Unsupported wire type encountered. Data is corrupted or deprecated wire type `GROUP` is used")
                    << TErrorAttribute("wireType", static_cast<int>(wireType))
                    << TErrorAttribute("fieldNumber", static_cast<int>(fieldNumber));
        }

        map[fieldNumber].emplace_back(stream.Checkpoint());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TWireStringPart::TWireStringPart()
{ }

TWireStringPart::TWireStringPart(const ui8* data, size_t size)
    : Span_(data, size)
{ }

bool TWireStringPart::IsEmpty() const
{
    return Span_.empty();
}

std::span<const ui8> TWireStringPart::AsSpan() const
{
    return Span_;
}

std::string_view TWireStringPart::AsStringView() const
{
    return {reinterpret_cast<const char*>(Span_.data()), Span_.size()};
}

TWireStringPart TWireStringPart::FromStringView(std::string_view view)
{
    return {reinterpret_cast<const ui8*>(view.data()), view.size()};
}

////////////////////////////////////////////////////////////////////////////////

const TWireString TWireString::Empty = {};

////////////////////////////////////////////////////////////////////////////////

TWireString TWireString::FromSerialized(std::string_view serializedProto)
{
    return {TWireStringPart::FromStringView(serializedProto)};
}

TWireString TWireString::FromSerialized(const std::vector<std::string_view>& serializedProtos)
{
    return FromSerializedRangeImpl(serializedProtos);
}

TWireString TWireString::FromSerialized(const std::vector<std::string>& serializedProtos)
{
    return FromSerializedRangeImpl(serializedProtos);
}

TWireString TWireString::FromSerialized(const std::vector<TString>& serializedProtos)
{
    return FromSerializedRangeImpl(serializedProtos);
}

bool TWireString::operator==(const TWireString& other) const
{
    auto selfView = std::ranges::views::join(std::ranges::views::transform(*this, &TWireStringPart::AsSpan));
    auto otherView = std::ranges::views::join(std::ranges::views::transform(other, &TWireStringPart::AsSpan));
    return std::ranges::equal(selfView, otherView);
}

TWireStringPart TWireString::LastOrEmptyPart() const
{
    return empty() ? TWireStringPart{} : back();
}

int TWireString::ByteLength() const
{
    int length = 0;
    for (const auto& wireStringPart : *this) {
        length += wireStringPart.AsSpan().size();
    }
    return length;
}

bool TWireString::IsEmpty() const
{
    return std::ranges::all_of(*this, &TWireStringPart::IsEmpty);
}

void TWireString::HeadAsHex(TStringBuilderBase* stringBuilder, std::optional<int> maxSize) const
{
    int processedSize = 0;
    for (const auto& wireStringPart : *this) {
        int currentSize = std::ssize(wireStringPart.AsStringView());
        if (maxSize) {
            if (*maxSize == processedSize) {
                break;
            }
            currentSize = std::min(currentSize, *maxSize - processedSize);
        }
        stringBuilder->AppendString(HexEncode(wireStringPart.AsStringView().substr(0, currentSize)));
        processedSize += currentSize;
    }
}

std::string TWireString::PrettyPrint() const
{
    static constexpr int DefaultWireStringHeadSize = 32;

    TStringBuilder builder;
    TDelimitedStringBuilderWrapper wrapper(&builder);

    auto byteLength = ByteLength();

    builder.AppendChar('{');
    wrapper->AppendFormat("ByteLength: %v", byteLength);
    wrapper->AppendFormat("Data: ");
    HeadAsHex(&builder, DefaultWireStringHeadSize);
    if (DefaultWireStringHeadSize < byteLength) {
        builder.AppendString("...");
    }
    builder.AppendChar('}');
    return builder.Flush();
}

THashMap<int, TWireString> TWireString::Unpack(
    const NProtoBuf::Descriptor* descriptor,
    const THashSet<int>& extractValuesFor) const
{
    auto doExtractValuesFor = [&extractValuesFor] (int fieldNumber) {
        return extractValuesFor.contains(fieldNumber);
    };

    return TUnpackedWireString{descriptor, *this, doExtractValuesFor}.FieldNumberToField();
}

// TProtoVisitorTraits implementation for TUnpackedWireString.
template<>
struct TProtoVisitorTraits<TUnpackedWireString>
{
    using TMessageParam = const TUnpackedWireString&;
    using TMessageReturn = TUnpackedWireString;

    static TErrorOr<const NProtoBuf::Descriptor*> GetDescriptor(TMessageParam message)
    {
        return message.GetDescriptor();
    }

    static TErrorOr<bool> IsSingularFieldPresent(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        return !message.GetFieldOrEmpty(fieldDescriptor).empty();
    }

    static TMessageReturn GetMessageFromSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        return {fieldDescriptor->message_type(), message.GetFieldOrEmpty(fieldDescriptor)};
    }

    static TErrorOr<int> GetRepeatedFieldSize(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        return std::ssize(message.GetFieldOrEmpty(fieldDescriptor));
    }

    static TMessageReturn GetMessageFromRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        return {fieldDescriptor->message_type(), {message.GetFieldOrEmpty(fieldDescriptor)[index]}};
    }

    static TError InsertRepeatedFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        Y_UNUSED(message);
        Y_UNUSED(fieldDescriptor);
        Y_UNUSED(index);

        // NB! Wire string is immutable.
        YT_ABORT();
    }

    static TErrorOr<TMessageReturn> InsertMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        std::unique_ptr<NProtoBuf::Message> keyMessage)
    {
        Y_UNUSED(message);
        Y_UNUSED(fieldDescriptor);
        Y_UNUSED(keyMessage);

        // NB! Wire string is immutable.
        YT_ABORT();
    }

    static TErrorOr<TMessageReturn> GetMessageFromMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        const NProtoBuf::Message* keyMessage)
    {
        const auto& mapWireString = message.GetFieldOrEmpty(fieldDescriptor);
        std::unique_ptr<NProtoBuf::Message> parsedMessage{keyMessage->New()};
        for (int index = std::ssize(mapWireString) - 1; index >= 0; --index) {
            if (!parsedMessage->ParseFromString(mapWireString[index].AsStringView())) {
                return TError(EErrorCode::InvalidData,
                    "Could not parse message from wire representation");
            }

            if (CompareScalarFields(
                parsedMessage.get(),
                parsedMessage->GetDescriptor()->map_key(),
                keyMessage,
                keyMessage->GetDescriptor()->map_key()) == std::partial_ordering::equivalent)
            {
                return TUnpackedWireString{keyMessage->GetDescriptor(), {mapWireString[index]}};
            }
        }

        return TError(EErrorCode::MissingKey,
            "Map item is not found");
    }

    using TMapReturn = THashMap<TString, TMessageReturn>;
    static TErrorOr<TMapReturn> GetMessagesFromWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TMapReturn result;
        std::unique_ptr<NProtoBuf::Message> parsedMessage{
            NProtoBuf::MessageFactory::generated_factory()->GetPrototype(fieldDescriptor->message_type())->New()};
        const auto& mapWireString = message.GetFieldOrEmpty(fieldDescriptor);
        for (int index = std::ssize(mapWireString) - 1; index >= 0; --index) {
            if (!parsedMessage->ParseFromString(mapWireString[index].AsStringView())) {
                return TError(EErrorCode::InvalidData,
                    "Could not parse message from wire representation");
            }
            auto packedValue = TUnpackedWireString(parsedMessage->GetDescriptor(), {mapWireString[index]})
                .GetFieldOrEmpty(parsedMessage->GetDescriptor()->map_value());
            auto errorOrKey = MapKeyFieldToString(parsedMessage.get(), parsedMessage->GetDescriptor()->map_key());
            if (!errorOrKey.IsOK()) {
                return TError(errorOrKey);
            }
            result.emplace(
                errorOrKey.Value(),
                TUnpackedWireString{parsedMessage->GetDescriptor()->map_value()->message_type(), packedValue});
        }

        return result;
    }

    static TMessageReturn GetDefaultMessage(
        TMessageParam message,
        const NProtoBuf::Descriptor* descriptor)
    {
        Y_UNUSED(message);

        return {descriptor, TWireString::Empty};
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWireStringVisitor final
    : public TProtoVisitor<TUnpackedWireString, TWireStringVisitor>
{
    friend class TProtoVisitor<TUnpackedWireString, TWireStringVisitor>;

public:
    TWireStringVisitor()
    {
        SetMissingFieldPolicy(EMissingFieldPolicy::Skip);
        SetProcessAttributeDictionary(true);
    }

    TWireString GetResult() &&
    {
        return Result_;
    }

    void VisitRegularMessage(
        const TUnpackedWireString& message,
        const NProtoBuf::Descriptor* descriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            Result_ = message.WireString();
            return;
        }

        TProtoVisitor::VisitRegularMessage(message, descriptor, reason);
    }

    void VisitAttributeDictionary(
        const TUnpackedWireString& message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        Y_UNUSED(fieldDescriptor);
        Y_UNUSED(reason);

        if (PathComplete()) {
            Result_ = message.WireString();
            return;
        }

        THROW_ERROR_EXCEPTION(EErrorCode::Unimplemented,
            "Slicing attribute dictionary via wire string is not supported");
    }

    void VisitRepeatedField(
        const TUnpackedWireString& message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            Result_ = message.GetFieldOrEmpty(fieldDescriptor);
            return;
        }

        TProtoVisitor::VisitRepeatedField(message, fieldDescriptor, reason);
    }

    void VisitRepeatedFieldEntry(
        const TUnpackedWireString& message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        if (PathComplete() && fieldDescriptor->cpp_type() != NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            Result_ = {message.GetFieldOrEmpty(fieldDescriptor)[index]};
            return;
        }

        TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }

    void VisitPresentSingularField(
        const TUnpackedWireString& message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
                TProtoVisitor::VisitPresentSingularField(
                    message,
                    fieldDescriptor,
                    EVisitReason::Manual);
            } else {
                Result_ = message.GetFieldOrEmpty(fieldDescriptor);
            }
            return;
        }

        TProtoVisitor::VisitPresentSingularField(message, fieldDescriptor, reason);
    }

    void VisitMapField(
        const TUnpackedWireString& message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            Result_ = message.GetFieldOrEmpty(fieldDescriptor);
            return;
        }

        TProtoVisitor::VisitMapField(message, fieldDescriptor, reason);
    }

protected:
    TWireString Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_PARSER_CASE(protoFieldType, intermediateType, stream, method, ...) \
    case protoFieldType: { \
        if (intermediateType result; (stream)->method(&result)) { \
            *value = __VA_ARGS__(result); \
            return (stream)->CurrentPosition() == std::ssize(wireStringPart.AsSpan()); \
        } \
        return false; \
    }

bool ParseUint32(ui32* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    switch (type.Underlying()) {
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_UINT32, ui32, &stream, ReadVarint32)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_FIXED32, ui32, &stream, ReadLittleEndian32)
        default:
            YT_ABORT();
    }
}

bool ParseUint64(ui64* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    switch (type.Underlying()) {
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_UINT32, ui32, &stream, ReadVarint32)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_UINT64, ui64, &stream, ReadVarint64)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_FIXED32, ui32, &stream, ReadLittleEndian32)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_FIXED64, ui64, &stream, ReadLittleEndian64)
        default:
            YT_ABORT();
    }
}

bool ParseInt32(i32* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    switch (type.Underlying()) {
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_INT32, ui32, &stream, ReadVarint32, std::bit_cast<i32>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_SINT32, ui32, &stream, ReadVarint32, ZigZagDecode32)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_SFIXED32, ui32, &stream, ReadLittleEndian32, std::bit_cast<i32>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_ENUM, ui32, &stream, ReadVarint32, std::bit_cast<i32>)
        default:
            YT_ABORT();
    }
}

bool ParseInt64(i64* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    switch (type.Underlying()) {
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_INT32, ui32, &stream, ReadVarint32, std::bit_cast<i32>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_INT64, ui64, &stream, ReadVarint64, std::bit_cast<i64>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_SINT32, ui32, &stream, ReadVarint32, ZigZagDecode32)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_SINT64, ui64, &stream, ReadVarint64, ZigZagDecode64)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_SFIXED32, ui32, &stream, ReadLittleEndian32, std::bit_cast<i32>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_SFIXED64, ui64, &stream, ReadLittleEndian64, std::bit_cast<i64>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_ENUM, ui32, &stream, ReadVarint32, std::bit_cast<i32>)
        default:
            YT_ABORT();
    }
}

bool ParseDouble(double* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    switch (type.Underlying()) {
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_FLOAT, ui32, &stream, ReadLittleEndian32, std::bit_cast<float>)
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_DOUBLE, ui64, &stream, ReadLittleEndian64, std::bit_cast<double>)
        default:
            YT_ABORT();
    }
}

bool ParseBoolean(bool* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    switch (type.Underlying()) {
        IMPLEMENT_PARSER_CASE(NProtoBuf::FieldDescriptor::TYPE_BOOL, ui32, &stream, ReadVarint32)
        default:
            YT_ABORT();
    }
}

std::pair<TWireStringPart, TWireStringPart> ParseKeyValuePair(TWireStringPart wireStringPart)
{
    THashMap<int, TWireString> unpackedPair;
    TUnpackedWireString::FillFieldNumberToFieldMap<
        /*TExtractValuesFor*/ NMpl::TConstantProjection<bool, true>,
        /*TGetPackedFieldType*/ NMpl::TConstantProjection<std::nullopt_t, std::nullopt>>(unpackedPair, wireStringPart);

    return {unpackedPair[MapKeyFieldNumber].LastOrEmptyPart(), unpackedPair[MapValueFieldNumber].LastOrEmptyPart()};
}

NYTree::IAttributeDictionaryPtr ParseAttributeDictionary(const TWireString& wireString)
{
    NYTree::NProto::TAttributeDictionary attributeDictionaryMessage;
    MergeMessageFrom(&attributeDictionaryMessage, wireString);
    return NYTree::FromProto(attributeDictionaryMessage);
}

#undef IMPLEMENT_PARSER_CASE

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_SERIALIZE_CASE(type, call) \
    case type: { \
        google::protobuf::io::StringOutputStream stringStream(&result); \
        google::protobuf::io::CodedOutputStream stream(&stringStream); \
        WireFormatLite::call(value, &stream); \
        break; \
    }

std::string SerializeUint64(ui64 value, NYson::TProtobufElementType type)
{
    TString result;
    switch (type.Underlying()) {
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_UINT32, WriteUInt32NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_UINT64, WriteUInt64NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_FIXED32, WriteFixed32NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_FIXED64, WriteFixed64NoTag)
        default:
            YT_ABORT();
    }
    return result;
}

std::string SerializeInt64(i64 value, NYson::TProtobufElementType type)
{
    TString result;
    switch (type.Underlying()) {
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_INT32, WriteInt32NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_INT64, WriteInt64NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_SINT32, WriteSInt32NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_SINT64, WriteSInt64NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_SFIXED32, WriteSFixed32NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_SFIXED64, WriteSFixed64NoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_ENUM, WriteEnumNoTag);
        default:
            YT_ABORT();
    }

    return result;
}

std::string SerializeDouble(double value, NYson::TProtobufElementType type)
{
    TString result;
    switch (type.Underlying()) {
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_FLOAT, WriteFloatNoTag)
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_DOUBLE, WriteDoubleNoTag)
        default:
            YT_ABORT();
    }

    return result;
}

std::string SerializeBoolean(bool value, NYson::TProtobufElementType type)
{
    TString result;
    switch (type.Underlying()) {
        IMPLEMENT_SERIALIZE_CASE(NProtoBuf::FieldDescriptor::TYPE_BOOL, WriteBoolNoTag)
        default:
            YT_ABORT();
    }

    return result;
}

std::string SerializeKeyValuePair(
    TWireStringPart key,
    NYson::TProtobufElementType keyType,
    const TWireString& value,
    NYson::TProtobufElementType valueType)
{
    TString result;
    {
        google::protobuf::io::StringOutputStream stringStream(&result);
        google::protobuf::io::CodedOutputStream stream(&stringStream);

        auto writeField = [&] (int fieldNumber, const TWireString& wireString, NYson::TProtobufElementType elementType) {
            auto wireType = WireFormatLite::WireTypeForFieldType(
                static_cast<WireFormatLite::FieldType>(elementType.Underlying()));
            WireFormatLite::WriteTag(fieldNumber, wireType, &stream);
            if (wireType == WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
                stream.WriteVarint32(wireString.ByteLength());
            }
            for (const auto& wireStringPart : wireString) {
                stream.WriteRaw(wireStringPart.AsSpan().data(), wireStringPart.AsSpan().size());
            }
        };

        writeField(MapKeyFieldNumber, TWireString{key}, keyType);
        writeField(MapValueFieldNumber, value, valueType);
    }

    return result;
}

std::string SerializeAttributeDictionary(const NYTree::IAttributeDictionary& attributeDictionary)
{
    NYTree::NProto::TAttributeDictionary attributeDictionaryMessage;
    NYTree::ToProto(&attributeDictionaryMessage, attributeDictionary);
    return attributeDictionaryMessage.SerializeAsString();
}

std::string SerializeMessage(
    const NYTree::INodePtr& message,
    const NYson::TProtobufMessageType* messageType,
    NYson::TProtobufWriterOptions options)
{
    TString result;
    google::protobuf::io::StringOutputStream stringStream(&result);
    auto protobufWriter = NYson::CreateProtobufWriter(&stringStream, messageType, options);
    NYTree::Serialize(message, protobufWriter.get());

    return result;
}

std::string SerializeMessage(
    const NYson::TYsonString& message,
    const NYson::TProtobufMessageType* messageType,
    NYson::TProtobufWriterOptions options)
{
    TString result;
    google::protobuf::io::StringOutputStream stringStream(&result);
    auto protobufWriter = NYson::CreateProtobufWriter(&stringStream, messageType, options);
    NYson::Serialize(message, protobufWriter.get());

    return result;
}

std::string AddWireTag(
    const NYson::TProtobufMessageType* messageType,
    std::string_view fieldName,
    const TString& serializedMessage)
{
    TString result;
    {
        google::protobuf::io::StringOutputStream stringStream(&result);
        google::protobuf::io::CodedOutputStream outputStream(&stringStream);

        const auto* descriptor = NYson::UnreflectProtobufMessageType(messageType);
        const auto* fieldDescriptor = descriptor->FindFieldByName(fieldName);
        THROW_ERROR_EXCEPTION_UNLESS(fieldDescriptor,
            "Protobuf message %Qv does not contain field %Qv",
            descriptor->full_name(),
            fieldName);

        WireFormatLite::WriteBytes(fieldDescriptor->number(), serializedMessage, &outputStream);
    }

    return result;
}


#undef DEFINE_STREAMS
#undef IMPLEMENT_SERIALIZE_CASE

////////////////////////////////////////////////////////////////////////////////

std::string ConvertMapKeyToWireString(
    std::string_view key,
    const NYson::TProtobufScalarElement& scalarElement)
{
    switch (scalarElement.Type.Underlying()) {
        case NProtoBuf::FieldDescriptor::TYPE_INT64:
        case NProtoBuf::FieldDescriptor::TYPE_INT32:
        case NProtoBuf::FieldDescriptor::TYPE_SINT32:
        case NProtoBuf::FieldDescriptor::TYPE_SINT64:
        case NProtoBuf::FieldDescriptor::TYPE_SFIXED32:
        case NProtoBuf::FieldDescriptor::TYPE_SFIXED64:
            if (i64 value; TryFromString(key, value)) {
                return SerializeInt64(value, scalarElement.Type);
            }
            break;
        case NProtoBuf::FieldDescriptor::TYPE_UINT64:
        case NProtoBuf::FieldDescriptor::TYPE_FIXED64:
        case NProtoBuf::FieldDescriptor::TYPE_UINT32:
        case NProtoBuf::FieldDescriptor::TYPE_FIXED32:
            if (ui64 value; TryFromString(key, value)) {
                return SerializeUint64(value, scalarElement.Type);
            }
            break;
        case NProtoBuf::FieldDescriptor::TYPE_BOOL:
            if (bool value; TryFromString(key, value)) {
                return SerializeBoolean(value, scalarElement.Type);
            }
            break;
        case NProtoBuf::FieldDescriptor::TYPE_STRING:
        case NProtoBuf::FieldDescriptor::TYPE_BYTES:
            return std::string{key};
        case NProtoBuf::FieldDescriptor::TYPE_ENUM:
            if (i64 value; TryFromString(key, value)) {
                return SerializeInt64(value, scalarElement.Type);
            } else {
                auto encodedEnum = NYson::FindProtobufEnumValueByLiteral<i32>(
                    scalarElement.EnumType,
                    key);
                THROW_ERROR_EXCEPTION_UNLESS(encodedEnum.has_value(),
                    "Literal %Qv is not a valid enum value",
                    key);
                return SerializeInt64(
                    *encodedEnum,
                    NYson::TProtobufElementType{google::protobuf::FieldDescriptor::TYPE_ENUM});

            }
            break;
        case NProtoBuf::FieldDescriptor::TYPE_DOUBLE:
        case NProtoBuf::FieldDescriptor::TYPE_FLOAT:
        case NProtoBuf::FieldDescriptor::TYPE_GROUP:
        case NProtoBuf::FieldDescriptor::TYPE_MESSAGE:
            break;
    }

    THROW_ERROR_EXCEPTION("Encountered map key of forbidden type")
        << TErrorAttribute("key", key)
        << TErrorAttribute("fieldType", scalarElement.Type.Underlying());
}

std::string ConvertScalarToWireString(
    const NYTree::INodePtr& value,
    const NYson::TProtobufScalarElement& scalarElement)
{
    if (scalarElement.Type.Underlying() == google::protobuf::FieldDescriptor::TYPE_ENUM) {
        if (value->GetType() == NYTree::ENodeType::String) {
            auto encodedEnum = NYson::FindProtobufEnumValueByLiteral<i32>(
                scalarElement.EnumType,
                value->AsString()->GetValue());
            THROW_ERROR_EXCEPTION_UNLESS(encodedEnum.has_value(),
                "Literal %Qv is not a valid enum value",
                value->AsString()->GetValue());
            return SerializeInt64(*encodedEnum, scalarElement.Type);
        }

        return SerializeInt64(value->GetValue<i64>(), scalarElement.Type);
    }

    auto expectedType = NYson::GetNodeTypeByProtobufScalarElement(scalarElement);
    switch (expectedType) {
        case NYTree::ENodeType::Entity:
            return {};
        case NYTree::ENodeType::Uint64: {
            return SerializeUint64(value->GetValue<ui64>(), scalarElement.Type);
        }
        case NYTree::ENodeType::Int64: {
            return SerializeInt64(value->GetValue<i64>(), scalarElement.Type);
        }
        case NYTree::ENodeType::Double: {
            return SerializeDouble(value->GetValue<double>(), scalarElement.Type);
        }
        case NYTree::ENodeType::Boolean: {
            return SerializeBoolean(value->GetValue<bool>(), scalarElement.Type);
        }
        case NYTree::ENodeType::String: {
            return value->AsString()->GetValue();
        }
        case NYTree::ENodeType::Composite:
        case NYTree::ENodeType::Map:
        case NYTree::ENodeType::List:
            break;
    }

    THROW_ERROR_EXCEPTION("Encountered non-scalar field type for scalar protobuf element")
        << TErrorAttribute("fieldType", scalarElement.Type.Underlying())
        << TErrorAttribute("nodeType", expectedType);
}

std::vector<std::string> ConvertToWireString(
    const NYTree::INodePtr& value,
    const NYson::TProtobufElement& element,
    const NYson::TProtobufWriterOptions& options)
{
    switch (value->GetType()) {
        case NYTree::ENodeType::Map: {
            return VisitProtobufElement(element,
                [&] (const NYson::TProtobufMessageElement& element) -> std::vector<std::string> {
                    return std::vector<std::string>{SerializeMessage(value, element.Type, options)};
                },
                [&] (const NYson::TProtobufAttributeDictionaryElement& /*element*/) -> std::vector<std::string> {
                    return {SerializeAttributeDictionary(*NYTree::IAttributeDictionary::FromMap(value->AsMap()))};
                },
                [&] (const NYson::TProtobufMapElement& element) {
                    std::vector<std::string> serializedMap;
                    serializedMap.reserve(value->AsMap()->GetChildCount());
                    for (const auto& [key, child] : value->AsMap()->GetChildren()) {
                        serializedMap.push_back(SerializeKeyValuePair(
                            TWireStringPart::FromStringView(ConvertMapKeyToWireString(key, element.KeyElement)),
                            element.KeyElement.Type,
                            TWireString::FromSerialized(ConvertToWireString(child, element.Element, options)),
                            GetProtobufElementType(element.Element)));
                    }
                    return serializedMap;
                },
                [&] <NYson::CProtobufElement TOtherProtobufElement>(const TOtherProtobufElement& /*element*/)
                    -> std::vector<std::string>
                {
                    THROW_ERROR_EXCEPTION(
                        "Encountered protobuf element of unexpected type %Qv in yson-map to wire string conversion",
                        NYson::GetProtobufElementTypeName(element));
                }
            );
        }
        case NYTree::ENodeType::List: {
            const auto& repeatedElement = NYson::GetProtobufElementOrThrow<NYson::TProtobufRepeatedElement>(element);
            std::vector<std::string> serializedRepeated;
            serializedRepeated.reserve(value->AsList()->GetChildCount());
            for (const auto& [index, child] : SEnumerate(value->AsList()->GetChildren())) {
                auto serializedScalar = ConvertToWireString(
                    child,
                    repeatedElement.Element,
                    options.CreateChildOptions(ToString(index)));
                serializedRepeated.insert(
                    serializedRepeated.end(),
                    std::make_move_iterator(serializedScalar.begin()),
                    std::make_move_iterator(serializedScalar.end()));
            }
            return serializedRepeated;
        }
        case NYTree::ENodeType::Entity:
            return {};
        case NYTree::ENodeType::Uint64:
        case NYTree::ENodeType::Int64:
        case NYTree::ENodeType::Double:
        case NYTree::ENodeType::Boolean:
        case NYTree::ENodeType::String: {
            return {ConvertScalarToWireString(
                value,
                NYson::GetProtobufElementOrThrow<NYson::TProtobufScalarElement>(element))};
        }
        case NYTree::ENodeType::Composite:
            THROW_ERROR_EXCEPTION(EErrorCode::InvalidData,
                "Cannot convert node of type %Qv to wire string",
                NYTree::ENodeType::Composite);
    }
}

////////////////////////////////////////////////////////////////////////////////

TWireString FlattenCopyWireStringTo(std::string* buffer, const TWireString& wireString)
{
    buffer->resize(wireString.ByteLength());
    TWireString result;
    result.reserve(wireString.size());
    int offset = 0;
    for (const auto& wireStringPart : wireString) {
        auto partSize = wireStringPart.AsStringView().size();
        std::memcpy(buffer->data() + offset, wireStringPart.AsStringView().data(), partSize);
        result.push_back(TWireStringPart::FromStringView({buffer->data() + offset, partSize}));
        offset += partSize;
    }

    return result;
}

TWireString FlattenCopyWireStringTo(TString* buffer, const TWireString& wireString)
{
    return FlattenCopyWireStringTo(&buffer->MutRef(), wireString);
}

////////////////////////////////////////////////////////////////////////////////

void MergeMessageFrom(NProtoBuf::MessageLite* message, TWireStringPart wireStringPart)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        message->MergeFromString(wireStringPart.AsStringView()),
        NAttributes::EErrorCode::InvalidData,
        "Could not parse message of type %v from wire representation",
        message->GetTypeName());
}

void MergeMessageFrom(NProtoBuf::MessageLite* message, const TWireString& wireString)
{
    for (const auto wireStringPart : wireString) {
        MergeMessageFrom(message, wireStringPart);
    }
}

////////////////////////////////////////////////////////////////////////////////

TWireString GetWireStringByPath(
    const NProtoBuf::Descriptor* descriptor,
    const TWireString& wireString,
    NYPath::TYPathBuf path)
{
    TWireStringVisitor visitor;
    visitor.Visit(TUnpackedWireString{descriptor, wireString}, path);
    return std::move(visitor).GetResult();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
