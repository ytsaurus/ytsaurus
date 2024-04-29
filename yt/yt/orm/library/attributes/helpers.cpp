#include "helpers.h"

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>

#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/error.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NOrm::NAttributes {

using namespace NYPath;
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
            .IndexType = EListIndexType::Relative
        };
    } else if (token == NYPath::ListEndToken) {
        return TIndexParseResult{
            .Index = count,
            .IndexType = EListIndexType::Relative
        };
    } else if (token.StartsWith(NYPath::ListBeforeToken) || token.StartsWith(NYPath::ListAfterToken)) {
        auto index = parseAbsoluteIndex(NYPath::ExtractListIndex(token));

        if (token.StartsWith(NYPath::ListAfterToken)) {
            ++index;
        }
        return TIndexParseResult{
            .Index = index,
            .IndexType = EListIndexType::Relative
        };
    } else {
        return TIndexParseResult{
            .Index = parseAbsoluteIndex(token),
            .IndexType = EListIndexType::Absolute
        };
    }
}

////////////////////////////////////////////////////////////////////////////////

TError CombineErrors(TError error1, TError error2)
{
    if (error1.IsOK()) {
        return error2;
    } else if (error2.IsOK()) {
        return error1;
    } else {
        return std::move(error1) << std::move(error2);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::partial_ordering CompareScalarFields(
    const Message* message1,
    const FieldDescriptor* fieldDescriptor1,
    const Message* message2,
    const FieldDescriptor* fieldDescriptor2)
{
    if (fieldDescriptor1->cpp_type() != fieldDescriptor2->cpp_type()) {
        return std::partial_ordering::unordered;
    }

    auto* reflection1 = message1->GetReflection();
    auto* reflection2 = message2->GetReflection();

    switch (fieldDescriptor1->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32:
            return reflection1->GetInt32(*message1, fieldDescriptor1)
                <=> reflection2->GetInt32(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_UINT32:
            return reflection1->GetUInt32(*message1, fieldDescriptor1)
                <=> reflection2->GetUInt32(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_INT64:
            return reflection1->GetInt64(*message1, fieldDescriptor1)
                <=> reflection2->GetInt64(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_UINT64:
            return reflection1->GetUInt64(*message1, fieldDescriptor1)
                <=> reflection2->GetUInt64(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return reflection1->GetDouble(*message1, fieldDescriptor1)
                <=> reflection2->GetDouble(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return reflection1->GetFloat(*message1, fieldDescriptor1)
                <=> reflection2->GetFloat(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_BOOL:
            return reflection1->GetBool(*message1, fieldDescriptor1)
                <=> reflection2->GetBool(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_ENUM:
            return reflection1->GetEnumValue(*message1, fieldDescriptor1)
                <=> reflection2->GetEnumValue(*message2, fieldDescriptor2);
        case FieldDescriptor::CppType::CPPTYPE_STRING: {
            TString scratch1;
            TString scratch2;
            return
                reflection1->GetStringReference(*message1, fieldDescriptor1, &scratch1).ConstRef()
                <=>
                reflection2->GetStringReference(*message2, fieldDescriptor2, &scratch2).ConstRef();
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
            return TError(EErrorCode::InvalidMap,
                "Fields of type %v are not supported as map keys",
                keyFieldDescriptor->type_name());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
