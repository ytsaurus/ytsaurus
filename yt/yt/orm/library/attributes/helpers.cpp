#include "helpers.h"

#include "wire_string.h"

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt_proto/yt/core/yson/proto/protobuf_interop.pb.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/cast.h>

#include <util/string/cast.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <google/protobuf/descriptor.pb.h>

namespace NYT::NOrm::NAttributes {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NProtoBuf::EnumValueDescriptor;
using NProtoBuf::FieldDescriptor;
using NProtoBuf::Message;
using NProtoBuf::UnknownField;
using NProtoBuf::UnknownFieldSet;
using NProtoBuf::io::CodedOutputStream;
using NProtoBuf::io::StringOutputStream;
using NProtoBuf::internal::WireFormatLite;

constexpr int ProtobufMapKeyFieldNumber = 1;
constexpr int ProtobufMapValueFieldNumber = 2;

////////////////////////////////////////////////////////////////////////////////

const TProtobufMessageType* GetMessageTypeByYPath(
    const TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    bool allowAttributeDictionary)
{
    auto result = ResolveProtobufElementByYPath(rootType, path);
    auto* messageElement = std::get_if<std::unique_ptr<TProtobufMessageElement>>(&result.Element);
    if (messageElement) {
        return (*messageElement)->Type;
    }
    if (allowAttributeDictionary) {
        auto* attributeDictionaryElement =
            std::get_if<std::unique_ptr<TProtobufAttributeDictionaryElement>>(&result.Element);
        if (attributeDictionaryElement) {
            return (*attributeDictionaryElement)->Type;
        }
    }
    THROW_ERROR_EXCEPTION("Attribute %v is not a protobuf message",
        result.HeadPath);
}

NYTree::INodePtr ConvertProtobufToNode(
    const TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    const TString& payload)
{
    const auto* payloadType = GetMessageTypeByYPath(rootType, path, /*allowAttributeDictionary*/ false);
    google::protobuf::io::ArrayInputStream protobufInputStream(payload.data(), payload.length());

    auto builder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
    builder->BeginTree();
    ParseProtobuf(&*builder, &protobufInputStream, payloadType);
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

// Returns [index of the item, its content].
TErrorOr<std::pair<int, TYsonString>> LookupUnknownYsonFieldsItem(
    UnknownFieldSet* unknownFields,
    TStringBuf key)
{
    int count = unknownFields->field_count();
    for (int index = 0; index < count; ++index) {
        auto* field = unknownFields->mutable_field(index);
        if (field->number() == UnknownYsonFieldNumber) {
            if (field->type() != UnknownField::TYPE_LENGTH_DELIMITED) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected type %v of item within yson unknown field set",
                    static_cast<int>(field->type()));
            }

            UnknownFieldSet tmpItem;
            if (!tmpItem.ParseFromString(field->length_delimited())) {
                return TError(NAttributes::EErrorCode::InvalidData, "Cannot parse UnknownYsonFields item");
            }

            if (tmpItem.field_count() != 2) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected field count %v in item within yson unknown field set",
                    tmpItem.field_count());
            }

            auto* keyField = tmpItem.mutable_field(0);
            auto* valueField = tmpItem.mutable_field(1);
            if (keyField->number() != ProtobufMapKeyFieldNumber) {
                std::swap(keyField, valueField);
            }

            if (keyField->number() != ProtobufMapKeyFieldNumber) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected key tag %v of item within yson unknown field set",
                    keyField->number());
            }
            if (keyField->type() != UnknownField::TYPE_LENGTH_DELIMITED) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected key type %v of item within yson unknown field set",
                    static_cast<int>(keyField->type()));
            }

            if (valueField->number() != ProtobufMapValueFieldNumber) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected value tag %v of item within yson unknown field set",
                    valueField->number());
            }
            if (valueField->type() != UnknownField::TYPE_LENGTH_DELIMITED) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected value type %v of item within yson unknown field set",
                    static_cast<int>(valueField->type()));
            }

            if (keyField->length_delimited() == key) {
                return std::pair<int, TYsonString>{
                    index,
                    TYsonString(valueField->length_delimited())};
            }
        }
    }
    return TError(NAttributes::EErrorCode::MissingKey, "Unknown yson field not found");
}

TString SerializeUnknownYsonFieldsItem(TStringBuf key, TStringBuf value)
{
    TString output;
    StringOutputStream outputStream(&output);
    CodedOutputStream codedOutputStream(&outputStream);
    codedOutputStream.WriteTag(
        WireFormatLite::MakeTag(ProtobufMapKeyFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
    codedOutputStream.WriteVarint64(key.length());
    codedOutputStream.WriteRaw(key.data(), static_cast<int>(key.length()));
    codedOutputStream.WriteTag(
        WireFormatLite::MakeTag(ProtobufMapValueFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
    codedOutputStream.WriteVarint64(value.length());
    codedOutputStream.WriteRaw(value.data(), static_cast<int>(value.length()));
    return output;
}

////////////////////////////////////////////////////////////////////////////////

void TIndexParseResult::EnsureIndexType(EListIndexType indexType, TStringBuf path)
{
    THROW_ERROR_EXCEPTION_UNLESS(IndexType == indexType,
        "Error traversing path %Qv: index token must be %v",
        path,
        indexType);
}

void TIndexParseResult::EnsureIndexIsWithinBounds(int count, TStringBuf path)
{
    THROW_ERROR_EXCEPTION_IF(IsOutOfBounds(count),
        "Repeated field index at %Qv must be in range [-%v, %v), but got %v",
        path,
        count,
        count,
        Index);
}

bool TIndexParseResult::IsOutOfBounds(int count)
{
    return Index < 0 || Index > count || (Index == count && IndexType == EListIndexType::Absolute);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAttributesProcessingConsumer
    : public NYson::TYsonConsumerBase
{
public:
    TAttributesProcessingConsumer(NYson::IYsonConsumer* underlying, std::function<void()> reporter)
        : Underlying_(underlying)
        , Reporter_(std::move(reporter))
        , Forward_(underlying)
    { }

    void OnStringScalar(TStringBuf string) override final
    {
        if (Forward_) {
            Forward_->OnStringScalar(string);
        }
    };

    void OnBeginList() override final
    {
        if (Forward_) {
            Forward_->OnBeginList();
        }
    };

    void OnListItem() override final
    {
        if (Forward_) {
            Forward_->OnListItem();
        }
    };

    void OnEndList() override final
    {
        if (Forward_) {
            Forward_->OnEndList();
        }
    };

    void OnBeginMap() override final
    {
        if (Forward_) {
            Forward_->OnBeginMap();
        }
    };

    void OnEndMap() override final
    {
        if (Forward_) {
            Forward_->OnEndMap();
        }
    };

    void OnKeyedItem(TStringBuf key) override final
    {
        if (Forward_) {
            Forward_->OnKeyedItem(key);
        }
    };

    void OnInt64Scalar(i64 value) override final
    {
        if (Forward_) {
            Forward_->OnInt64Scalar(value);
        }
    };

    void OnUint64Scalar(ui64 value) override final
    {
        if (Forward_) {
            Forward_->OnUint64Scalar(value);
        }
    };

    void OnDoubleScalar(double value) override final
    {
        if (Forward_) {
            Forward_->OnDoubleScalar(value);
        }
    };

    void OnBooleanScalar(bool value) override final
    {
        if (Forward_) {
            Forward_->OnBooleanScalar(value);
        }
    };

    void OnEntity() override final
    {
        if (Forward_) {
            Forward_->OnEntity();
        }
    };

    void OnBeginAttributes() override final
    {
        if (Reporter_) {
            Reporter_();
        }

        ++AttributesDepth_;
        Forward_ = nullptr;
    };

    void OnEndAttributes() override final
    {
        --AttributesDepth_;
        if (AttributesDepth_ == 0) {
            Forward_ = Underlying_;
        }
    };

private:
    NYson::IYsonConsumer* const Underlying_;
    const std::function<void()> Reporter_;

    NYson::IYsonConsumer* Forward_;
    int AttributesDepth_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateAttributesDetectingConsumer(
    std::function<void()> reporter)
{
    return std::make_unique<TAttributesProcessingConsumer>(/*underlying*/ nullptr, std::move(reporter));
}

std::unique_ptr<NYson::IYsonConsumer> CreateAttributesRemovingConsumer(
    NYson::IYsonConsumer* underlying,
    std::function<void()> reporter)
{
    return std::make_unique<TAttributesProcessingConsumer>(underlying, std::move(reporter));
}

////////////////////////////////////////////////////////////////////////////////

TIndexParseResult ParseListIndex(TStringBuf token, int count)
{
    auto parseAbsoluteIndex = [count] (TStringBuf token) {
        int index = NYPath::ParseListIndex(token);
        if (index < 0) {
            index += count;
        }
        return index;
    };

    if (token == NYPath::ListBeginToken) {
        return TIndexParseResult{
            .Index = 0,
            .IndexType = EListIndexType::Relative,
        };
    } else if (token == NYPath::ListEndToken) {
        return TIndexParseResult{
            .Index = count,
            .IndexType = EListIndexType::Relative,
        };
    } else if (token.StartsWith(NYPath::ListBeforeToken) || token.StartsWith(NYPath::ListAfterToken)) {
        auto index = parseAbsoluteIndex(NYPath::ExtractListIndex(token));

        if (token.StartsWith(NYPath::ListAfterToken)) {
            ++index;
        }
        return TIndexParseResult{
            .Index = index,
            .IndexType = EListIndexType::Relative,
        };
    } else {
        return TIndexParseResult{
            .Index = parseAbsoluteIndex(token),
            .IndexType = EListIndexType::Absolute,
        };
    }
}

////////////////////////////////////////////////////////////////////////////////

// The error coalescing makes sure the top-level error code is either common to all errors or is the
// mismatch code. This is important for comparisons.
void ReduceErrors(TError& base, TError incoming, NAttributes::EErrorCode mismatchErrorCode)
{
    if (base.IsOK()) {
        YT_VERIFY(!incoming.IsOK());
        base = TError(mismatchErrorCode, "Some messages have errors")
            << std::move(incoming);
    } else if (incoming.IsOK()) {
        base = TError(mismatchErrorCode, "Some messages have errors")
            << std::move(base);
    } else if (base.GetCode() == mismatchErrorCode) {
        base <<= std::move(incoming);
    } else if (base.GetCode() == incoming.GetCode()) {
        base <<= std::move(incoming);
    } else {
        base = TError(mismatchErrorCode, "Some messages have errors")
            << std::move(base)
            << std::move(incoming);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::partial_ordering CompareScalarFields(
    const Message* lhsMessage,
    const FieldDescriptor* lhsFieldDescriptor,
    const Message* rhsMessage,
    const FieldDescriptor* rhsFieldDescriptor)
{
    if (lhsFieldDescriptor->cpp_type() != rhsFieldDescriptor->cpp_type()) {
        return std::partial_ordering::unordered;
    }

    auto* lhsReflection = lhsMessage->GetReflection();
    auto* rhsReflection = rhsMessage->GetReflection();

    switch (lhsFieldDescriptor->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32:
            return lhsReflection->GetInt32(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetInt32(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_UINT32:
            return lhsReflection->GetUInt32(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetUInt32(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_INT64:
            return lhsReflection->GetInt64(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetInt64(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_UINT64:
            return lhsReflection->GetUInt64(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetUInt64(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return lhsReflection->GetDouble(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetDouble(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return lhsReflection->GetFloat(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetFloat(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_BOOL:
            return lhsReflection->GetBool(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetBool(*rhsMessage, rhsFieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_ENUM: {
            if (lhsFieldDescriptor->enum_type() != rhsFieldDescriptor->enum_type()) {
                return std::partial_ordering::unordered;
            }
            return lhsReflection->GetEnumValue(*lhsMessage, lhsFieldDescriptor)
                <=> rhsReflection->GetEnumValue(*rhsMessage, rhsFieldDescriptor);
        }
        case FieldDescriptor::CppType::CPPTYPE_STRING: {
            TString lhsScratch;
            TString rhsScratch;
            return lhsReflection->
                GetStringReference(*lhsMessage, lhsFieldDescriptor, &lhsScratch).ConstRef()
                <=> rhsReflection->
                GetStringReference(*rhsMessage, rhsFieldDescriptor, &rhsScratch).ConstRef();
        }
        default:
            return std::partial_ordering::unordered;
    }
}

std::partial_ordering CompareScalarRepeatedFieldEntries(
    const Message* lhsMessage,
    const FieldDescriptor* lhsFieldDescriptor,
    int lhsIndex,
    const Message* rhsMessage,
    const FieldDescriptor* rhsFieldDescriptor,
    int rhsIndex)
{
    if (lhsFieldDescriptor->cpp_type() != rhsFieldDescriptor->cpp_type()) {
        return std::partial_ordering::unordered;
    }

    auto* lhsReflection = lhsMessage->GetReflection();
    auto* rhsReflection = rhsMessage->GetReflection();

    switch (lhsFieldDescriptor->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32:
            return lhsReflection->GetRepeatedInt32(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedInt32(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_UINT32:
            return lhsReflection->GetRepeatedUInt32(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedUInt32(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_INT64:
            return lhsReflection->GetRepeatedInt64(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedInt64(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_UINT64:
            return lhsReflection->GetRepeatedUInt64(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedUInt64(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return lhsReflection->GetRepeatedDouble(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedDouble(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return lhsReflection->GetRepeatedFloat(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedFloat(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_BOOL:
            return lhsReflection->GetRepeatedBool(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedBool(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        case FieldDescriptor::CppType::CPPTYPE_ENUM: {
            if (lhsFieldDescriptor->enum_type() != rhsFieldDescriptor->enum_type()) {
                return std::partial_ordering::unordered;
            }
            return lhsReflection->GetRepeatedEnumValue(*lhsMessage, lhsFieldDescriptor, lhsIndex)
                <=> rhsReflection->GetRepeatedEnumValue(*rhsMessage, rhsFieldDescriptor, rhsIndex);
        }
    case FieldDescriptor::CppType::CPPTYPE_STRING: {
        TString lhsScratch;
        TString rhsScratch;
        return lhsReflection-> GetRepeatedStringReference(
            *lhsMessage,
            lhsFieldDescriptor,
            lhsIndex,
            &lhsScratch).ConstRef()
            <=> rhsReflection->GetRepeatedStringReference(
            *rhsMessage,
            rhsFieldDescriptor,
            rhsIndex,
            &rhsScratch).ConstRef();
    }
    default:
        return std::partial_ordering::unordered;
    }
}

////////////////////////////////////////////////////////////////////////////////

TErrorOr<int> LocateMapEntry(
    const Message* message,
    const FieldDescriptor* fieldDescriptor,
    const Message* keyMessage)
{
    YT_VERIFY(fieldDescriptor->is_map());

    auto* reflection = message->GetReflection();
    auto* keyFieldDescriptor = fieldDescriptor->message_type()->map_key();
    int size = reflection->FieldSize(*message, fieldDescriptor);

    for (int index = 0; index < size; ++index) {
        auto* entryMessage = &reflection->GetRepeatedMessage(*message, fieldDescriptor, index);
        auto* entryKeyFieldDescriptor = entryMessage->GetDescriptor()->map_key();
        if (CompareScalarFields(
            entryMessage,
            entryKeyFieldDescriptor,
            keyMessage,
            keyFieldDescriptor) == std::partial_ordering::equivalent)
        {
            return index;
        }
    }

    return TError(NAttributes::EErrorCode::MissingKey, "Key not found in map");
}

TErrorOr<TString> MapKeyFieldToString(
    const Message* message,
    const FieldDescriptor* keyFieldDescriptor)
{
    auto* reflection = message->GetReflection();

    switch (keyFieldDescriptor->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32:
            return ToString(reflection->GetInt32(*message, keyFieldDescriptor));
        case FieldDescriptor::CppType::CPPTYPE_UINT32:
            return ToString(reflection->GetUInt32(*message, keyFieldDescriptor));
        case FieldDescriptor::CppType::CPPTYPE_INT64:
            return ToString(reflection->GetInt64(*message, keyFieldDescriptor));
        case FieldDescriptor::CppType::CPPTYPE_UINT64:
            return ToString(reflection->GetUInt64(*message, keyFieldDescriptor));
        case FieldDescriptor::CppType::CPPTYPE_STRING:
            return reflection->GetString(*message, keyFieldDescriptor);
        default:
            return TError(NAttributes::EErrorCode::InvalidData,
                "Fields of type %v are not supported as map keys",
                keyFieldDescriptor->type_name());
    }
}

TErrorOr<std::unique_ptr<Message>> MakeMapKeyMessage(
    const FieldDescriptor* fieldDescriptor,
    TString key)
{
    auto* descriptor = fieldDescriptor->message_type();
    std::unique_ptr<Message> result{
        NProtoBuf::MessageFactory::generated_factory()->GetPrototype(descriptor)->New()};

    auto* keyFieldDescriptor = descriptor->map_key();
    auto error = SetScalarFieldFromString(result.get(), keyFieldDescriptor, std::move(key));

    if (!error.IsOK()) {
        return error;
    }
    return std::move(result);
}

////////////////////////////////////////////////////////////////////////////////

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    const NYTree::INodePtr& value)
{
    switch (value->GetType()) {
        case NYTree::ENodeType::String:
            return SetScalarField(message, fieldDescriptor, value->AsString()->GetValue());
        case NYTree::ENodeType::Int64:
            return SetScalarField(message, fieldDescriptor, value->AsInt64()->GetValue());
        case NYTree::ENodeType::Uint64:
            return SetScalarField(message, fieldDescriptor, value->AsUint64()->GetValue());
        case NYTree::ENodeType::Double:
            return SetScalarField(message, fieldDescriptor, value->AsDouble()->GetValue());
        case NYTree::ENodeType::Boolean:
            return SetScalarField(message, fieldDescriptor, value->AsBoolean()->GetValue());
        case NYTree::ENodeType::Entity:
            return SetDefaultScalarFieldValue(message, fieldDescriptor);
        default:
            return TError(NAttributes::EErrorCode::Unimplemented,
                "Cannot convert yson value of type %v to a proto field",
                value->GetType());
    }
}

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    const TWireString& wireString)
{
    auto wireStringPart = wireString.LastOrEmptyPart();
    if (wireStringPart.AsSpan().empty()) {
        return SetDefaultScalarFieldValue(message, fieldDescriptor);
    }

    switch (fieldDescriptor->cpp_type()) {
        case NProtoBuf::FieldDescriptor::CPPTYPE_STRING:
            return SetScalarField(message, fieldDescriptor, TString{wireStringPart.AsStringView()});
        case NProtoBuf::FieldDescriptor::CPPTYPE_INT32:
        case NProtoBuf::FieldDescriptor::CPPTYPE_INT64:
        case NProtoBuf::FieldDescriptor::CPPTYPE_ENUM:
            if (i64 value; ParseInt64(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarField(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_UINT32:
        case NProtoBuf::FieldDescriptor::CPPTYPE_UINT64:
            if (ui64 value; ParseUint64(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarField(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_DOUBLE:
        case NProtoBuf::FieldDescriptor::CPPTYPE_FLOAT:
            if (double value; ParseDouble(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarField(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_BOOL:
            if (bool value; ParseBoolean(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarField(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE:
            YT_ABORT();
    }

    return TError(NAttributes::EErrorCode::InvalidData,
        "Cannot parse %v from wire string",
        google::protobuf::FieldDescriptor::TypeName(fieldDescriptor->type()));
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    const NYTree::INodePtr& value)
{
    switch (value->GetType()) {
        case NYTree::ENodeType::String:
            return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value->AsString()->GetValue());
        case NYTree::ENodeType::Int64:
            return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value->AsInt64()->GetValue());
        case NYTree::ENodeType::Uint64:
            return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value->AsUint64()->GetValue());
        case NYTree::ENodeType::Double:
            return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value->AsDouble()->GetValue());
        case NYTree::ENodeType::Boolean:
            return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value->AsBoolean()->GetValue());
        case NYTree::ENodeType::Entity:
            return SetDefaultScalarRepeatedFieldEntryValue(message, fieldDescriptor, index);
        default:
            return TError(NAttributes::EErrorCode::Unimplemented,
                "Cannot convert yson value of type %v to a proto field",
                value->GetType());
    }
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    const TWireString& wireString)
{
    auto wireStringPart = wireString.LastOrEmptyPart();

    if (wireStringPart.AsSpan().empty()) {
        return SetDefaultScalarRepeatedFieldEntryValue(message, fieldDescriptor, index);
    }

    switch (fieldDescriptor->cpp_type()) {
        case NProtoBuf::FieldDescriptor::CPPTYPE_STRING:
            return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, TString{wireStringPart.AsStringView()});
        case NProtoBuf::FieldDescriptor::CPPTYPE_INT32:
        case NProtoBuf::FieldDescriptor::CPPTYPE_INT64:
        case NProtoBuf::FieldDescriptor::CPPTYPE_ENUM:
            if (i64 value; ParseInt64(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_UINT32:
        case NProtoBuf::FieldDescriptor::CPPTYPE_UINT64:
            if (ui64 value; ParseUint64(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_DOUBLE:
        case NProtoBuf::FieldDescriptor::CPPTYPE_FLOAT:
            if (double value; ParseDouble(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_BOOL:
            if (bool value; ParseBoolean(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE:
            YT_ABORT();
    }

    return TError(NAttributes::EErrorCode::InvalidData,
        "Cannot parse %v from wire string",
        google::protobuf::FieldDescriptor::TypeName(fieldDescriptor->type()));
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    const NYTree::INodePtr& value)
{
    switch (value->GetType()) {
        case NYTree::ENodeType::String:
            return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value->AsString()->GetValue());
        case NYTree::ENodeType::Int64:
            return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value->AsInt64()->GetValue());
        case NYTree::ENodeType::Uint64:
            return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value->AsUint64()->GetValue());
        case NYTree::ENodeType::Double:
            return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value->AsDouble()->GetValue());
        case NYTree::ENodeType::Boolean:
            return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value->AsBoolean()->GetValue());
        case NYTree::ENodeType::Entity:
            return AddDefaultScalarFieldEntryValue(message, fieldDescriptor);
        default:
            return TError(NAttributes::EErrorCode::Unimplemented,
                "Cannot convert yson value of type %v to a proto field",
                value->GetType());
    }
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    TWireStringPart wireStringPart)
{
    if (wireStringPart.AsSpan().empty()) {
        return AddDefaultScalarFieldEntryValue(message, fieldDescriptor);
    }

    switch (fieldDescriptor->cpp_type()) {
        case NProtoBuf::FieldDescriptor::CPPTYPE_STRING:
            return AddScalarRepeatedFieldEntry(message, fieldDescriptor, TString{wireStringPart.AsStringView()});
        case NProtoBuf::FieldDescriptor::CPPTYPE_INT32:
        case NProtoBuf::FieldDescriptor::CPPTYPE_INT64:
        case NProtoBuf::FieldDescriptor::CPPTYPE_ENUM:
            if (i64 value; ParseInt64(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_UINT32:
        case NProtoBuf::FieldDescriptor::CPPTYPE_UINT64:
            if (ui64 value; ParseUint64(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_DOUBLE:
        case NProtoBuf::FieldDescriptor::CPPTYPE_FLOAT:
            if (double value; ParseDouble(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_BOOL:
            if (bool value; ParseBoolean(&value, TProtobufElementType{fieldDescriptor->type()}, wireStringPart)) {
                return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value);
            }
            break;
        case NProtoBuf::FieldDescriptor::CPPTYPE_MESSAGE:
            YT_ABORT();
    }

    return TError(NAttributes::EErrorCode::InvalidData,
        "Cannot parse %v from wire string",
        NProtoBuf::FieldDescriptor::TypeName(fieldDescriptor->type()));
}

std::pair<int, TError> FindAttributeDictionaryEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const TString& key)
{
    const auto* entryDescriptor = fieldDescriptor->message_type();
    if (entryDescriptor == nullptr) {
        return {
            -1,
            TError("Failed to locate the descriptor of an attribute dictionary entry")
        };
    }
    const auto* keyFieldDescriptor = entryDescriptor->FindFieldByName("key");
    if (keyFieldDescriptor == nullptr) {
        return {
            -1,
            TError("Failed to locate the key field in an attribute dictionary entry")
        };
    }

    const auto* reflection = message->GetReflection();

    int size = reflection->FieldSize(*message, fieldDescriptor);
    for (int i = 0; i < size; ++i) {
        const auto& entry = reflection->GetRepeatedMessage(*message, fieldDescriptor, i);
        const auto* entryReflection = entry.GetReflection();
        TString tmp;
        const auto& entryKey =
            entryReflection->GetStringReference(entry, keyFieldDescriptor, &tmp);
        if (entryKey == key) {
            return {i, TError()};
        }
        if (entryKey > key) {
            return {
                i,
                TError(NAttributes::EErrorCode::MissingKey, "Attribute dictionary does not contain the key")
            };
        }
    }

    return {
        size,
        TError(NAttributes::EErrorCode::MissingKey, "Attribute dictionary does not contain the key")
    };
}

TErrorOr<TYsonString> GetAttributeDictionaryEntryValue(const NProtoBuf::Message* entry)
{
    const auto* entryDescriptor = entry->GetDescriptor();
    const auto* entryReflection = entry->GetReflection();

    const auto* valueFieldDescriptor = entryDescriptor->FindFieldByName("value");
    if (valueFieldDescriptor == nullptr) {
        return TError("Failed to locate the value field in an attribute dictionary entry");
    }
    TString tmp;
    const auto& value = entryReflection->GetStringReference(*entry, valueFieldDescriptor, &tmp);

    return TYsonString(value);
}

TError SetAttributeDictionaryEntryValue(
    NProtoBuf::Message* entry,
    const TYsonString& value)
{
    const auto* entryDescriptor = entry->GetDescriptor();
    const auto* entryReflection = entry->GetReflection();

    const auto* valueFieldDescriptor = entryDescriptor->FindFieldByName("value");
    if (valueFieldDescriptor == nullptr) {
        return TError("Failed to locate the value field in an attribute dictionary entry");
    }
    entryReflection->SetString(entry, valueFieldDescriptor, value.ToString());

    return TError();
}

TErrorOr<NProtoBuf::Message*> AddAttributeDictionaryEntry(
    NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    const TString& key)
{
    const auto* reflection = message->GetReflection();

    auto* entry = reflection->AddMessage(message, fieldDescriptor);
    const auto* entryDescriptor = entry->GetDescriptor();
    const auto* entryReflection = entry->GetReflection();

    const auto* keyFieldDescriptor = entryDescriptor->FindFieldByName("key");
    if (keyFieldDescriptor == nullptr) {
        return TError("Failed to locate the key field in an attribute dictionary entry");
    }
    entryReflection->SetString(entry, keyFieldDescriptor, key);

    return entry;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
const EnumValueDescriptor* LookupEnumValue(
    const FieldDescriptor* fieldDescriptor,
    const T& value)
{
    const auto* enumDescriptor = fieldDescriptor->enum_type();
    if constexpr (std::is_integral_v<T>) {
        return enumDescriptor->FindValueByNumber(value);
    } else { // TString
        auto decoded = TryDecodeEnumValue(value);
        for (int i = 0; i < enumDescriptor->value_count(); ++i) {
            const auto* enumValueDescriptor = enumDescriptor->value(i);
            if (enumValueDescriptor->options().HasExtension(NYT::NYson::NProto::enum_value_name) &&
                enumValueDescriptor->options().GetExtension(NYT::NYson::NProto::enum_value_name) == value)
            {
                return enumValueDescriptor;
            }
            if (decoded.has_value() && enumValueDescriptor->name() == decoded.value()) {
                return enumValueDescriptor;
            }
            if (enumValueDescriptor->name() == value) {
                return enumValueDescriptor;
            }
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

#define BEGIN_SWITCH(snakeType) \
    const auto* reflection = message->GetReflection(); \
    switch (fieldDescriptor->cpp_type()) { \
        static_assert(true)

#define _CAST(snakeType) \
    auto optionalCastValue = TryCheckedIntegralCast<snakeType>(value); \
    if (!optionalCastValue) { \
        return TError(NAttributes::EErrorCode::InvalidData, \
            "Value %v does not fit in " #snakeType, \
            value); \
    } \
    const auto& castValue = *optionalCastValue; \
    static_assert(true)

#define _CAST_FROM_STRING(snakeType) \
    snakeType castValue; \
    if (!TryFromString(value, castValue)) { \
        return TError(NAttributes::EErrorCode::InvalidData, \
            "Cannot convert %v to a " #snakeType, \
            value); \
    } \
    static_assert(true)

#define _CAST_ENUM() \
    const auto* enumValue = LookupEnumValue(fieldDescriptor, value); \
    if (!enumValue) { \
        return TError(NAttributes::EErrorCode::InvalidData, \
            "Failed to convert %v to a %v enum", \
            value, \
            fieldDescriptor->enum_type()->full_name()); \
    } \
    static_assert(true)

#define CASE_SCALAR_WITH_CAST(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            { \
                _CAST(snakeType); \
                reflection->Set##camelType(message, fieldDescriptor, castValue); \
            } \
            break;

#define CASE_SCALAR_WITH_CAST_FROM_STRING(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            { \
                _CAST_FROM_STRING(snakeType); \
                reflection->Set##camelType(message, fieldDescriptor, castValue); \
            } \
            break;

#define CASE_SCALAR_DIRECT(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            reflection->Set##camelType(message, fieldDescriptor, value); \
            break

#define CASE_SCALAR_ENUM() \
        case FieldDescriptor::CPPTYPE_ENUM: \
            { \
                _CAST_ENUM(); \
                reflection->SetEnum(message, fieldDescriptor, enumValue); \
            } \
            break

#define CASE_SCALAR_DEFAULT(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            reflection->Set##camelType( \
                message, \
                fieldDescriptor, \
                fieldDescriptor->default_value_##snakeType()); \
            break

#define CASE_REPEATED_WITH_CAST(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            { \
                _CAST(snakeType); \
                reflection->SetRepeated##camelType(message, fieldDescriptor, index, castValue); \
            } \
            break

#define CASE_REPEATED_WITH_CAST_FROM_STRING(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            { \
                _CAST_FROM_STRING(snakeType); \
                reflection->SetRepeated##camelType(message, fieldDescriptor, index, castValue); \
            } \
            break

#define CASE_REPEATED_DIRECT(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            reflection->SetRepeated##camelType(message, fieldDescriptor, index, value); \
            break

#define CASE_REPEATED_ENUM() \
            case FieldDescriptor::CPPTYPE_ENUM: \
            { \
                _CAST_ENUM(); \
                reflection->SetRepeatedEnum(message, fieldDescriptor, index, enumValue); \
            } \
            break

#define CASE_REPEATED_DEFAULT(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            reflection->SetRepeated##camelType( \
                message, \
                fieldDescriptor, \
                index, \
                fieldDescriptor->default_value_##snakeType()); \
            break

#define CASE_ADD_WITH_CAST(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            { \
                _CAST(snakeType); \
                reflection->Add##camelType(message, fieldDescriptor, castValue); \
            } \
            break

#define CASE_ADD_WITH_CAST_FROM_STRING(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            { \
                _CAST_FROM_STRING(snakeType); \
                reflection->Add##camelType(message, fieldDescriptor, castValue); \
            } \
            break

#define CASE_ADD_DIRECT(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            reflection->Add##camelType(message, fieldDescriptor, value); \
            break

#define CASE_ADD_ENUM() \
        case FieldDescriptor::CPPTYPE_ENUM: \
        { \
            _CAST_ENUM(); \
            reflection->AddEnum(message, fieldDescriptor, enumValue); \
        } \
        break

#define CASE_ADD_DEFAULT(snakeType, camelType, capsType) \
        case FieldDescriptor::CPPTYPE_##capsType: \
            reflection->Add##camelType( \
                message, \
                fieldDescriptor, \
                fieldDescriptor->default_value_##snakeType()); \
            break

#define END_SWITCH(snakeType) \
        default: \
            return TError(NAttributes::EErrorCode::InvalidData, \
                "Cannot convert " #snakeType " to a proto field of type %v", \
                fieldDescriptor->type_name()); \
    } \
    return TError()

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    i64 value)
{
    BEGIN_SWITCH(i64);
        CASE_SCALAR_WITH_CAST(i32, Int32, INT32);
        CASE_SCALAR_DIRECT(i64, Int64, INT64);
        CASE_SCALAR_WITH_CAST(ui32, UInt32, UINT32);
        CASE_SCALAR_WITH_CAST(ui64, UInt64, UINT64);
        CASE_SCALAR_DIRECT(double, Double, DOUBLE);
        CASE_SCALAR_DIRECT(float, Float, FLOAT);
        CASE_SCALAR_ENUM();
    END_SWITCH(i64);
}

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    ui64 value)
{
    BEGIN_SWITCH(ui64);
        CASE_SCALAR_WITH_CAST(i32, Int32, INT32);
        CASE_SCALAR_WITH_CAST(i64, Int64, INT64);
        CASE_SCALAR_WITH_CAST(ui32, UInt32, UINT32);
        CASE_SCALAR_DIRECT(ui64, UInt64, UINT64);
        CASE_SCALAR_DIRECT(double, Double, DOUBLE);
        CASE_SCALAR_DIRECT(float, Float, FLOAT);
        CASE_SCALAR_ENUM();
    END_SWITCH(ui64);
}

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    double value)
{
    BEGIN_SWITCH(double);
        CASE_SCALAR_DIRECT(double, Double, DOUBLE);
        CASE_SCALAR_DIRECT(float, Float, FLOAT);
    END_SWITCH(double);
}

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    bool value)
{
    BEGIN_SWITCH(bool);
        CASE_SCALAR_DIRECT(bool, Bool, BOOL);
    END_SWITCH(bool);
}

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    TString value)
{
    BEGIN_SWITCH(TString);
        CASE_SCALAR_DIRECT(TString, String, STRING);
        CASE_SCALAR_ENUM();
    END_SWITCH(TString);
}

TError SetScalarFieldFromString(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    TString value)
{
    BEGIN_SWITCH(TString);
        CASE_SCALAR_WITH_CAST_FROM_STRING(i32, Int32, INT32);
        CASE_SCALAR_WITH_CAST_FROM_STRING(i64, Int64, INT64);
        CASE_SCALAR_WITH_CAST_FROM_STRING(ui32, UInt32, UINT32);
        CASE_SCALAR_WITH_CAST_FROM_STRING(ui64, UInt64, UINT64);
        CASE_SCALAR_WITH_CAST_FROM_STRING(double, Double, DOUBLE);
        CASE_SCALAR_WITH_CAST_FROM_STRING(float, Float, FLOAT);
        CASE_SCALAR_WITH_CAST_FROM_STRING(bool, Bool, BOOL);
        CASE_SCALAR_DIRECT(TString, String, STRING);
        CASE_SCALAR_ENUM();
    END_SWITCH(TString);
}

TError SetDefaultScalarFieldValue(
    Message* message,
    const FieldDescriptor* fieldDescriptor)
{
    BEGIN_SWITCH(default);
        CASE_SCALAR_DEFAULT(int32, Int32, INT32);
        CASE_SCALAR_DEFAULT(int64, Int64, INT64);
        CASE_SCALAR_DEFAULT(uint32, UInt32, UINT32);
        CASE_SCALAR_DEFAULT(uint64, UInt64, UINT64);
        CASE_SCALAR_DEFAULT(double, Double, DOUBLE);
        CASE_SCALAR_DEFAULT(float, Float, FLOAT);
        CASE_SCALAR_DEFAULT(bool, Bool, BOOL);
        CASE_SCALAR_DEFAULT(string, String, STRING);
        CASE_SCALAR_DEFAULT(enum, Enum, ENUM);
    END_SWITCH(default);
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    i64 value)
{
    BEGIN_SWITCH(i64);
        CASE_REPEATED_WITH_CAST(i32, Int32, INT32);
        CASE_REPEATED_DIRECT(i64, Int64, INT64);
        CASE_REPEATED_WITH_CAST(ui32, UInt32, UINT32);
        CASE_REPEATED_WITH_CAST(ui64, UInt64, UINT64);
        CASE_REPEATED_DIRECT(double, Double, DOUBLE);
        CASE_REPEATED_DIRECT(float, Float, FLOAT);
        CASE_REPEATED_ENUM();
    END_SWITCH(i64);
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    ui64 value)
{
    BEGIN_SWITCH(ui64);
        CASE_REPEATED_WITH_CAST(i32, Int32, INT32);
        CASE_REPEATED_WITH_CAST(i64, Int64, INT64);
        CASE_REPEATED_WITH_CAST(ui32, UInt32, UINT32);
        CASE_REPEATED_DIRECT(ui64, UInt64, UINT64);
        CASE_REPEATED_DIRECT(double, Double, DOUBLE);
        CASE_REPEATED_DIRECT(float, Float, FLOAT);
        CASE_REPEATED_ENUM();
    END_SWITCH(ui64);
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    double value)
{
    BEGIN_SWITCH(double);
        CASE_REPEATED_DIRECT(double, Double, DOUBLE);
        CASE_REPEATED_DIRECT(float, Float, FLOAT);
    END_SWITCH(double);
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    bool value)
{
    BEGIN_SWITCH(bool);
        CASE_REPEATED_DIRECT(bool, Bool, BOOL);
    END_SWITCH(bool);
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    TString value)
{
    BEGIN_SWITCH(TString);
        CASE_REPEATED_DIRECT(TString, String, STRING);
        CASE_REPEATED_ENUM();
    END_SWITCH(TString);
}

TError SetScalarRepeatedFieldEntryFromString(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    TString value)
{
    BEGIN_SWITCH(TString);
        CASE_REPEATED_WITH_CAST_FROM_STRING(i32, Int32, INT32);
        CASE_REPEATED_WITH_CAST_FROM_STRING(i64, Int64, INT64);
        CASE_REPEATED_WITH_CAST_FROM_STRING(ui32, UInt32, UINT32);
        CASE_REPEATED_WITH_CAST_FROM_STRING(ui64, UInt64, UINT64);
        CASE_REPEATED_WITH_CAST_FROM_STRING(double, Double, DOUBLE);
        CASE_REPEATED_WITH_CAST_FROM_STRING(float, Float, FLOAT);
        CASE_REPEATED_WITH_CAST_FROM_STRING(bool, Bool, BOOL);
        CASE_REPEATED_DIRECT(TString, String, STRING);
        CASE_REPEATED_ENUM();
    END_SWITCH(TString);
}

TError SetDefaultScalarRepeatedFieldEntryValue(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index)
{
    BEGIN_SWITCH(default);
        CASE_REPEATED_DEFAULT(int32, Int32, INT32);
        CASE_REPEATED_DEFAULT(int64, Int64, INT64);
        CASE_REPEATED_DEFAULT(uint32, UInt32, UINT32);
        CASE_REPEATED_DEFAULT(uint64, UInt64, UINT64);
        CASE_REPEATED_DEFAULT(double, Double, DOUBLE);
        CASE_REPEATED_DEFAULT(float, Float, FLOAT);
        CASE_REPEATED_DEFAULT(bool, Bool, BOOL);
        CASE_REPEATED_DEFAULT(string, String, STRING);
        CASE_REPEATED_DEFAULT(enum, Enum, ENUM);
    END_SWITCH(default);
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    i64 value)
{
    BEGIN_SWITCH(i64);
        CASE_ADD_WITH_CAST(i32, Int32, INT32);
        CASE_ADD_DIRECT(i64, Int64, INT64);
        CASE_ADD_WITH_CAST(ui32, UInt32, UINT32);
        CASE_ADD_WITH_CAST(ui64, UInt64, UINT64);
        CASE_ADD_DIRECT(double, Double, DOUBLE);
        CASE_ADD_DIRECT(float, Float, FLOAT);
        CASE_ADD_ENUM();
    END_SWITCH(i64);
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    ui64 value)
{
    BEGIN_SWITCH(ui64);
        CASE_ADD_WITH_CAST(i32, Int32, INT32);
        CASE_ADD_WITH_CAST(i64, Int64, INT64);
        CASE_ADD_WITH_CAST(ui32, UInt32, UINT32);
        CASE_ADD_DIRECT(ui64, UInt64, UINT64);
        CASE_ADD_DIRECT(double, Double, DOUBLE);
        CASE_ADD_DIRECT(float, Float, FLOAT);
        CASE_ADD_ENUM();
    END_SWITCH(ui64);
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    double value)
{
    BEGIN_SWITCH(double);
        CASE_ADD_DIRECT(double, Double, DOUBLE);
        CASE_ADD_DIRECT(float, Float, FLOAT);
    END_SWITCH(double);
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    bool value)
{
    BEGIN_SWITCH(bool);
        CASE_ADD_DIRECT(bool, Bool, BOOL);
    END_SWITCH(bool);
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    TString value)
{
    BEGIN_SWITCH(TString);
        CASE_ADD_DIRECT(TString, String, STRING);
        CASE_ADD_ENUM();
    END_SWITCH(TString);
}

TError AddScalarRepeatedFieldEntryFromString(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    TString value)
{
    BEGIN_SWITCH(TString);
        CASE_ADD_WITH_CAST_FROM_STRING(i32, Int32, INT32);
        CASE_ADD_WITH_CAST_FROM_STRING(i64, Int64, INT64);
        CASE_ADD_WITH_CAST_FROM_STRING(ui32, UInt32, UINT32);
        CASE_ADD_WITH_CAST_FROM_STRING(ui64, UInt64, UINT64);
        CASE_ADD_WITH_CAST_FROM_STRING(double, Double, DOUBLE);
        CASE_ADD_WITH_CAST_FROM_STRING(float, Float, FLOAT);
        CASE_ADD_WITH_CAST_FROM_STRING(bool, Bool, BOOL);
        CASE_ADD_DIRECT(TString, String, STRING);
        CASE_ADD_ENUM();
    END_SWITCH(TString);
}

TError AddDefaultScalarFieldEntryValue(
    Message* message,
    const FieldDescriptor* fieldDescriptor)
{
    BEGIN_SWITCH(default);
        CASE_ADD_DEFAULT(int32, Int32, INT32);
        CASE_ADD_DEFAULT(int64, Int64, INT64);
        CASE_ADD_DEFAULT(uint32, UInt32, UINT32);
        CASE_ADD_DEFAULT(uint64, UInt64, UINT64);
        CASE_ADD_DEFAULT(double, Double, DOUBLE);
        CASE_ADD_DEFAULT(float, Float, FLOAT);
        CASE_ADD_DEFAULT(bool, Bool, BOOL);
        CASE_ADD_DEFAULT(string, String, STRING);
        CASE_ADD_DEFAULT(enum, Enum, ENUM);
    END_SWITCH(default);
}

////////////////////////////////////////////////////////////////////////////////

void RotateLastEntryBeforeIndex(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index)
{
    const auto* reflection = message->GetReflection();
    int last = reflection->FieldSize(*message, fieldDescriptor) - 1;
    for (int pos = index; pos < last; ++pos) {
        reflection->SwapElements(message, fieldDescriptor, pos, last);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TBooleanOrCollector::operator() ()
{
    Value_ = true;
}

bool TBooleanOrCollector::operator() (bool value)
{
    Value_ |= value;
    return value;
}

bool TBooleanOrCollector::Result() &&
{
    return Value_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
