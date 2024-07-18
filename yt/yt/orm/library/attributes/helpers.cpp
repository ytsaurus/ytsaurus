#include "helpers.h"

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/error.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NOrm::NAttributes {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NProtoBuf::FieldDescriptor;
using NProtoBuf::Message;

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

TYsonStringWriterHelper::TYsonStringWriterHelper(EYsonFormat format, EYsonType type)
    : Output_(ValueString_)
    , Writer_(CreateYsonWriter(&Output_, format, type, /*enableRaw*/ format == EYsonFormat::Binary))
{ }

IYsonConsumer* TYsonStringWriterHelper::GetConsumer()
{
    return Writer_.get();
}

TYsonString TYsonStringWriterHelper::Flush()
{
    Writer_->Flush();
    auto result = TYsonString(ValueString_);
    ValueString_.clear();
    return result;
}

bool TYsonStringWriterHelper::IsEmpty() const
{
    return ValueString_.Empty();
}

////////////////////////////////////////////////////////////////////////////////

void TIndexParseResult::EnsureIndexType(EListIndexType indexType, TStringBuf path)
{
    THROW_ERROR_EXCEPTION_UNLESS(IndexType == indexType,
        "Error traversing path %Qv: index token must be %v",
        path,
        indexType);
}

void TIndexParseResult::EnsureIndexIsWithinBounds(i64 count, TStringBuf path)
{
    THROW_ERROR_EXCEPTION_IF(IsOutOfBounds(count),
        "Repeated field index at %Qv must be in range [-%v, %v), but got %v",
        path,
        count,
        count,
        Index);
}

bool TIndexParseResult::IsOutOfBounds(i64 count)
{
    return Index < 0 || Index > count || (Index == count && IndexType == EListIndexType::Absolute);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAttributesDetectingConsumer
    : public NYson::IYsonConsumer
{
public:
    TAttributesDetectingConsumer(std::function<void()> reporter)
        : Reporter_(std::move(reporter))
    { }

    void OnStringScalar(TStringBuf /*string*/) override final
    { };

    void OnBeginList() override final
    { };

    void OnListItem() override final
    { };

    void OnEndList() override final
    { };

    void OnBeginMap() override final
    { };

    void OnKeyedItem(TStringBuf /*key*/) override final
    { };

    void OnEndMap() override final
    { };

    void OnInt64Scalar(i64 /*value*/) override final
    { };

    void OnUint64Scalar(ui64 /*value*/) override final
    { };

    void OnDoubleScalar(double /*value*/) override final
    { };

    void OnBooleanScalar(bool /*value*/) override final
    { };

    void OnEntity() override final
    { };

    void OnRaw(TStringBuf /*yson*/, NYson::EYsonType /*type*/) override final
    { };

    void OnBeginAttributes() override final
    {
        Reporter_();
    };

    void OnEndAttributes() override final
    { };

private:
    const std::function<void()> Reporter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateAttributesDetectingConsumer(std::function<void()> callback)
{
    return std::make_unique<TAttributesDetectingConsumer>(std::move(callback));
}

////////////////////////////////////////////////////////////////////////////////

TIndexParseResult ParseListIndex(TStringBuf token, i64 count)
{
    auto parseAbsoluteIndex = [count] (TStringBuf token) {
        i64 index = NYPath::ParseListIndex(token);
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
void ReduceErrors(TError& base, TError incoming, EErrorCode mismatchErrorCode)
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

    return TError(EErrorCode::MissingKey, "Key not found in map");
}

////////////////////////////////////////////////////////////////////////////////

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
            return TError(EErrorCode::InvalidData,
                "Fields of type %v are not supported as map keys",
                keyFieldDescriptor->type_name());
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
T ConvertToScalarValue(const NYTree::INodePtr& node, const T& defaultValue)
{
    if (node->GetType() == ENodeType::Entity) {
        return defaultValue;
    }
    return ConvertTo<T>(node);
}

int ConvertToEnumValue(const NYTree::INodePtr& node, const FieldDescriptor* field)
{
    YT_VERIFY(field->cpp_type() == FieldDescriptor::CPPTYPE_ENUM);
    if (node->GetType() == ENodeType::Entity) {
        return field->default_value_enum()->number();
    }
    return ConvertToProtobufEnumValue<int>(ReflectProtobufEnumType(field->enum_type()), node);
}

} // namespace

TError SetScalarField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    const NYTree::INodePtr& value)
{
    const auto* reflection = message->GetReflection();
    try {
        switch (fieldDescriptor->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->SetInt32(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_int32()));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->SetInt64(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_int64()));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->SetUInt32(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_uint32()));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->SetUInt64(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_uint64()));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->SetDouble(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_double()));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->SetFloat(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue<double>(value, fieldDescriptor->default_value_float()));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->SetBool(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_bool()));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection->SetEnumValue(
                    message,
                    fieldDescriptor,
                    ConvertToEnumValue(value, fieldDescriptor));
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->SetString(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_string()));
                break;
            default:
                return TError(EErrorCode::Unimplemented,
                    "Cannot convert yson value to a proto field of type %v",
                    fieldDescriptor->type_name());
        }
    } catch (std::exception& ex) {
        return TError("Failed to convert yson value to a proto field") << ex;
    }

    return TError();
}

TError SetScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    const NYTree::INodePtr& value)
{
    const auto* reflection = message->GetReflection();
    try {
        switch (fieldDescriptor->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->SetRepeatedInt32(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_int32()));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->SetRepeatedInt64(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_int64()));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->SetRepeatedUInt32(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_uint32()));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->SetRepeatedUInt64(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_uint64()));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->SetRepeatedDouble(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_double()));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->SetRepeatedFloat(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue<double>(value, fieldDescriptor->default_value_float()));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->SetRepeatedBool(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_bool()));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection->SetRepeatedEnumValue(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToEnumValue(value, fieldDescriptor));
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->SetRepeatedString(
                    message,
                    fieldDescriptor,
                    index,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_string()));
                break;
            default:
                return TError(EErrorCode::Unimplemented,
                    "Cannot convert yson value to a proto field of type %v",
                    fieldDescriptor->type_name());
        }
    } catch (std::exception& ex) {
        return TError("Failed to convert yson value to a proto field") << ex;
    }

    return TError();
}

TError AddScalarRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    const NYTree::INodePtr& value)
{
    const auto* reflection = message->GetReflection();
    try {
        switch (fieldDescriptor->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->AddInt32(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_int32()));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->AddInt64(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_int64()));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->AddUInt32(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_uint32()));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->AddUInt64(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_uint64()));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->AddDouble(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_double()));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->AddFloat(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue<double>(value, fieldDescriptor->default_value_float()));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->AddBool(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_bool()));
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection->AddEnumValue(
                    message,
                    fieldDescriptor,
                    ConvertToEnumValue(value, fieldDescriptor));
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->AddString(
                    message,
                    fieldDescriptor,
                    ConvertToScalarValue(value, fieldDescriptor->default_value_string()));
                break;
            default:
                return TError(EErrorCode::Unimplemented,
                    "Cannot convert yson value to a proto field of type %v",
                    fieldDescriptor->type_name());
        }
    } catch (std::exception& ex) {
        return TError("Failed to convert yson value to a proto field") << ex;
    }

    return TError();
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
                TError(EErrorCode::MissingKey, "Attribute dictionary does not contain the key")
            };
        }
    }

    return {
        size,
        TError(EErrorCode::MissingKey, "Attribute dictionary does not contain the key")
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

} // namespace NYT::NOrm::NAttributes
