#include "wire_string.h"

#include <yt/yt/orm/library/attributes/proto_visitor.h>

#include <ranges>

namespace NYT::NOrm::NAttributes {

using google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <std::ranges::range TRange>
TWireString FromSerializedRangeImpl(TRange serializedRange)
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
    using TTagToFieldMap = THashMap<int, TWireString>;

    explicit TUnpackedWireString(const NProtoBuf::Descriptor* descriptor)
        : Descriptor_(descriptor)
    { }

    TUnpackedWireString(const NProtoBuf::Descriptor* descriptor, TWireString wireString)
        : Descriptor_(descriptor)
        , WireString_(std::move(wireString))
    {
        YT_VERIFY(Descriptor_);
        for (const auto& wireStringPart : WireString_) {
            FillTagToFieldMap(wireStringPart);
        }
    }

    const TWireString& GetFieldOrEmpty(const NProtoBuf::FieldDescriptor* field) const
    {
        auto iter = TagToField_.find(field->number());
        if (iter == TagToField_.end()) {
            return TWireString::Empty;
        }

        return iter->second;
    }

    DEFINE_BYVAL_RO_PROPERTY(const NProtoBuf::Descriptor*, Descriptor);
    DEFINE_BYREF_RO_PROPERTY(TWireString, WireString);
    DEFINE_BYREF_RO_PROPERTY(TTagToFieldMap, TagToField);

private:
    void FillTagToFieldMap(TWireStringPart wireStringPart);
};

////////////////////////////////////////////////////////////////////////////////

class TCodedInputStream
    : public google::protobuf::io::CodedInputStream
{
public:
    TCodedInputStream(TWireStringPart wireStringPart)
        : google::protobuf::io::CodedInputStream(wireStringPart.data(), wireStringPart.size())
        , Data_(wireStringPart.data())
    { }

    TWireStringPart Checkpoint()
    {
        auto newCheckpoint = CurrentPosition();
        TWireStringPart result{Data_ + LastCheckpoint_, static_cast<size_t>(newCheckpoint - LastCheckpoint_)};
        LastCheckpoint_ = newCheckpoint;
        return result;
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

void TUnpackedWireString::FillTagToFieldMap(
    TWireStringPart wireStringPart)
{
    TCodedInputStream stream(wireStringPart);
    while (ui32 tag = stream.ReadTag()) {
        auto wireType = WireFormatLite::GetTagWireType(tag);
        auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);

        std::optional<WireFormatLite::WireType> packedFieldWireType;
        if (auto* field = GetDescriptor()->FindFieldByNumber(fieldNumber);
            field && NProtoBuf::FieldDescriptor::IsTypePackable(field->type()))
        {
            packedFieldWireType = WireFormatLite::WireTypeForFieldType(
                static_cast<WireFormatLite::FieldType>(field->type()));
        }

        switch (wireType) {
            case WireFormatLite::WIRETYPE_VARINT: {
                ui64 dummyVarint;
                stream.Checkpoint();
                stream.ReadVarint64(&dummyVarint);
                break;
            }
            case WireFormatLite::WIRETYPE_FIXED64: {
                ui64 dummyFixed64;
                stream.Checkpoint();
                stream.ReadLittleEndian64(&dummyFixed64);
                break;
            }
            case WireFormatLite::WIRETYPE_FIXED32: {
                ui32 dummyFixed32;
                stream.Checkpoint();
                stream.ReadLittleEndian32(&dummyFixed32);
                break;
            }
            case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                int size;
                stream.ReadVarintSizeAsInt(&size);
                stream.Checkpoint();
                stream.Skip(size);

                if (packedFieldWireType) {
                    UnpackRepeatedField(TagToField_[fieldNumber], stream.Checkpoint(), *packedFieldWireType);
                    continue;
                }
                break;
            }
            default:
                THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::InvalidData,
                    "Unsupported wire type encountered. Data is corrupted or deprecated type `GROUP` is used");
        }

        TagToField_[fieldNumber].emplace_back(stream.Checkpoint());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TWireStringPart::TWireStringPart()
{ }

TWireStringPart::TWireStringPart(const ui8* data, size_t size)
    : std::span<const ui8>(data, size)
{ }

std::string_view TWireStringPart::AsStringView() const
{
    return {reinterpret_cast<const char*>(data()), size()};
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

TWireString TWireString::FromSerialized(const std::vector<TString>& serializedProtos)
{
    return FromSerializedRangeImpl(serializedProtos);
}

bool TWireString::operator==(const TWireString& other) const
{
    auto selfView = std::ranges::views::join(*this);
    auto otherView = std::ranges::views::join(other);
    return std::lexicographical_compare_three_way(
        selfView.begin(),
        selfView.end(),
        otherView.begin(),
        otherView.end())== std::strong_ordering::equal;
}

TWireStringPart TWireStringPart::Skip(int count) const
{
    return TWireStringPart{data() + count, size() - count};
}

////////////////////////////////////////////////////////////////////////////////

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

void MergeMessageFrom(NProtoBuf::MessageLite* message, const TWireString& wireString)
{
    for (const auto wireStringPart : wireString) {
        THROW_ERROR_EXCEPTION_UNLESS(
            message->MergeFromString(wireStringPart.AsStringView()),
            NAttributes::EErrorCode::InvalidData,
            "Could not parse message of type %v from wire representation",
            message->GetTypeName());
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
