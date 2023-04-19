#include "scalar_attribute.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/misc/cast.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>
#include <google/protobuf/unknown_field_set.h>
#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/wire_format.h>

#include <variant>

namespace NYT::NOrm::NAttributes {

namespace {

////////////////////////////////////////////////////////////////////////////////

using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::UnknownField;
using ::google::protobuf::UnknownFieldSet;
using ::google::protobuf::Reflection;
using ::google::protobuf::internal::WireFormatLite;
using ::google::protobuf::io::CodedOutputStream;
using ::google::protobuf::io::StringOutputStream;
using ::google::protobuf::util::FieldComparator;
using ::google::protobuf::util::MessageDifferencer;

using namespace NYson;
using namespace NYTree;

constexpr int ProtobufMapKeyFieldNumber = 1;
constexpr int ProtobufMapValueFieldNumber = 2;

////////////////////////////////////////////////////////////////////////////////

template <bool IsMutable>
struct TMutabilityTraits;

template <>
struct TMutabilityTraits</*IsMutable*/ true>
{
    using TGenericMessage = NProtoBuf::Message;

    static TGenericMessage* GetMessage(TGenericMessage* root)
    {
        return const_cast<TGenericMessage*>(root);
    }

    static TGenericMessage* GetRepeatedMessage(TGenericMessage* root, const FieldDescriptor* field, int index)
    {
        return root->GetReflection()->MutableRepeatedMessage(
            const_cast<TGenericMessage*>(root),
            field,
            index);
    }

    static TGenericMessage* GetMessage(TGenericMessage* root, const FieldDescriptor* field)
    {
        return root->GetReflection()->MutableMessage(const_cast<TGenericMessage*>(root), field);
    }
};

template <>
struct TMutabilityTraits</*IsMutable*/ false>
{
    using TGenericMessage = const NProtoBuf::Message;

    static TGenericMessage* GetMessage(const TGenericMessage* root)
    {
        return root;
    }

    static TGenericMessage* GetRepeatedMessage(TGenericMessage* root, const FieldDescriptor* field, int index)
    {
        return &root->GetReflection()->GetRepeatedMessage(
            *root,
            field,
            index);
    }

    static TGenericMessage* GetMessage(TGenericMessage* root, const FieldDescriptor* field)
    {
        return &root->GetReflection()->GetMessage(*root, field);
    }
};

////////////////////////////////////////////////////////////////////////////////

const TString& GetKey(
    const NProtoBuf::Message& message,
    const FieldDescriptor* field,
    TString& scratch)
{
    const auto* reflection = message.GetReflection();
    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            scratch = ToString(reflection->GetInt32(message, field));
            return scratch;
        case FieldDescriptor::CPPTYPE_INT64:
            scratch = ToString(reflection->GetInt64(message, field));
            return scratch;
        case FieldDescriptor::CPPTYPE_UINT32:
            scratch = ToString(reflection->GetUInt32(message, field));
            return scratch;
        case FieldDescriptor::CPPTYPE_UINT64:
            scratch = ToString(reflection->GetUInt64(message, field));
            return scratch;
        case FieldDescriptor::CPPTYPE_BOOL:
            scratch = ToString(reflection->GetBool(message, field));
            return scratch;
        case FieldDescriptor::CPPTYPE_STRING:
            return reflection->GetStringReference(message, field, &scratch);
        default:
            break;
    }
    THROW_ERROR_EXCEPTION("Unexpected map key type %v",
        static_cast<int>(field->cpp_type()));
}


void SetKey(
    NProtoBuf::Message* message,
    const FieldDescriptor* field,
    const TString& key)
{
    const auto* reflection = message->GetReflection();
    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            reflection->SetInt32(message, field, FromString<i32>(key));
            return;
        case FieldDescriptor::CPPTYPE_INT64:
            reflection->SetInt64(message, field, FromString<i64>(key));
            return;
        case FieldDescriptor::CPPTYPE_UINT32:
            reflection->SetUInt32(message, field, FromString<ui32>(key));
            return;
        case FieldDescriptor::CPPTYPE_UINT64:
            reflection->SetUInt64(message, field, FromString<ui64>(key));
            return;
        case FieldDescriptor::CPPTYPE_BOOL:
            reflection->SetBool(message, field, FromString<bool>(key));
            return;
        case FieldDescriptor::CPPTYPE_STRING:
            reflection->SetString(message, field, key);
            return;
        default:
            break;
    }
    THROW_ERROR_EXCEPTION("Unexpected map key type %v",
        static_cast<int>(field->cpp_type()));
}

template <bool IsMutable>
struct TLookupMapItemResult
{
    typename TMutabilityTraits<IsMutable>::TGenericMessage* Message;
    int Index;
};

template <bool IsMutable>
std::optional<TLookupMapItemResult<IsMutable>> LookupMapItem(
    typename TMutabilityTraits<IsMutable>::TGenericMessage* message,
    const FieldDescriptor* field,
    const TString& key)
{
    YT_VERIFY(field->is_map());
    const auto* keyType = field->message_type()->map_key();

    const auto* reflection = message->GetReflection();
    int count = reflection->FieldSize(*message, field);

    TString tmp;
    for (int index = 0; index < count; ++index) {
        auto* item = TMutabilityTraits<IsMutable>::GetRepeatedMessage(message, field, index);
        const TString& mapKey = GetKey(*item, keyType, tmp);
        if (mapKey == key) {
            return TLookupMapItemResult<IsMutable>{item, index};
        }
    }
    return std::nullopt;
}

NProtoBuf::Message* AddMapItem(NProtoBuf::Message* message, const FieldDescriptor* field, const TString& key)
{
    YT_VERIFY(field->is_map());
    const auto* keyType = field->message_type()->map_key();

    auto* item = message->GetReflection()->AddMessage(message, field);
    YT_VERIFY(item);

    SetKey(item, keyType, key);
    return item;
}

template <bool IsMutable>
std::optional<typename TMutabilityTraits<IsMutable>::TGenericMessage*> LookupMapItemValue(
    typename TMutabilityTraits<IsMutable>::TGenericMessage* message,
    const FieldDescriptor* field,
    const TString& key)
{
    auto itemIfPresent = LookupMapItem<IsMutable>(message, field, key);

    if (itemIfPresent) {
        auto item = itemIfPresent->Message;
        YT_ASSERT(field->message_type()->map_value()->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE);
        const auto* fieldDescriptor = field->message_type()->map_value();
        return TMutabilityTraits<IsMutable>::GetMessage(item, fieldDescriptor);
    } else {
        return std::nullopt;
    }
}

NProtoBuf::Message* AddMapItemValue(NProtoBuf::Message* message, const FieldDescriptor* field, const TString& key)
{
    auto* item = AddMapItem(message, field, key);
    YT_ASSERT(field->message_type()->map_value()->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE);
    return TMutabilityTraits</*IsMutable*/ true>::GetMessage(item, field->message_type()->map_value());
}

////////////////////////////////////////////////////////////////////////////////

std::optional<int> LookupUnknownYsonFieldsItem(UnknownFieldSet* unknownFields, TStringBuf key, TString& value)
{
    int count = unknownFields->field_count();
    for (int index = 0; index < count; ++index) {
        auto* field = unknownFields->mutable_field(index);
        if (field->number() == UnknownYsonFieldNumber) {
            UnknownFieldSet tmpItem;
            THROW_ERROR_EXCEPTION_UNLESS(field->type() == UnknownField::TYPE_LENGTH_DELIMITED,
                "Unexpected type %v of item within yson unknown field set",
                static_cast<int>(field->type()));
            THROW_ERROR_EXCEPTION_UNLESS(tmpItem.ParseFromString(field->length_delimited()),
                "Cannot parse UnknownYsonFields item");
            THROW_ERROR_EXCEPTION_UNLESS(tmpItem.field_count() == 2,
                "Unexpected field count %v in item within yson unknown field set",
                tmpItem.field_count());
            auto* keyField = tmpItem.mutable_field(0);
            auto* valueField = tmpItem.mutable_field(1);
            if (keyField->number() != ProtobufMapKeyFieldNumber) {
                std::swap(keyField, valueField);
            }
            THROW_ERROR_EXCEPTION_UNLESS(keyField->number() == ProtobufMapKeyFieldNumber,
                "Unexpected key tag %v of item within yson unknown field set",
                keyField->number());
            THROW_ERROR_EXCEPTION_UNLESS(valueField->number() == ProtobufMapValueFieldNumber,
                "Unexpected value tag %v of item within yson unknown field set",
                valueField->number());
            THROW_ERROR_EXCEPTION_UNLESS(keyField->type() == UnknownField::TYPE_LENGTH_DELIMITED,
                "Unexpected key type %v of item within yson unknown field set",
                static_cast<int>(keyField->type()));
            THROW_ERROR_EXCEPTION_UNLESS(valueField->type() == UnknownField::TYPE_LENGTH_DELIMITED,
                "Unexpected value type %v of item within yson unknown field set",
                static_cast<int>(valueField->type()));
            if (keyField->length_delimited() == key) {
                value = std::move(*valueField->mutable_length_delimited());
                return index;
            }
        }
    }
    return std::nullopt;
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

DEFINE_ENUM(EListIndexType,
    (Absolute)
    (Relative)
);

struct TParseIndexResult
{
    i64 Index;
    EListIndexType IndexType;

    void EnsureIndexType(EListIndexType indexType, TStringBuf path)
    {
        THROW_ERROR_EXCEPTION_UNLESS(IndexType == indexType,
            "Error traversing path %Qv: index token must be %v",
            path,
            indexType);
    }

    void EnsureIndexIsWithinBounds(i64 count, TStringBuf path)
    {
        THROW_ERROR_EXCEPTION_IF(IsOutOfBounds(count),
            "Repeated field index at %Qv must be in range [-%v, %v), but got %v",
            path,
            count,
            count,
            Index);
    }

    bool IsOutOfBounds(i64 count)
    {
        return Index < 0 || Index >= count;
    }

};

// Parses list index from 'end', 'begin', 'before:<index>', 'after:<index>' or Integer in [-count, count).
TParseIndexResult ParseListIndex(TStringBuf token, int count)
{
    auto parseAbsoluteIndex = [count] (TStringBuf token) {
        i64 index = NYPath::ParseListIndex(token);
        if (index < 0) {
            index += count;
        }
        return index;
    };

    if (token == NYPath::ListBeginToken) {
        return TParseIndexResult{
            .Index = 0,
            .IndexType = EListIndexType::Relative
        };
    } else if (token == NYPath::ListEndToken) {
        return TParseIndexResult{
            .Index = count,
            .IndexType = EListIndexType::Relative
        };
    } else if (token.StartsWith(NYPath::ListBeforeToken) || token.StartsWith(NYPath::ListAfterToken)) {
        auto index = parseAbsoluteIndex(NYPath::ExtractListIndex(token));

        if (token.StartsWith(NYPath::ListAfterToken)) {
            ++index;
        }
        return TParseIndexResult{
            .Index = index,
            .IndexType = EListIndexType::Relative
        };
    } else {
        return TParseIndexResult{
            .Index = parseAbsoluteIndex(token),
            .IndexType = EListIndexType::Absolute
        };
    }
}

////////////////////////////////////////////////////////////////////////////////

int ConvertToEnumValue(const INodePtr& node, const FieldDescriptor* field)
{
    YT_VERIFY(field->cpp_type() == FieldDescriptor::CPPTYPE_ENUM);
    return ConvertToProtobufEnumValue<int>(ReflectProtobufEnumType(field->enum_type()), node);
}

[[noreturn]] void ThrowUnsupportedCppType(google::protobuf::FieldDescriptor::CppType cppType, TStringBuf path)
{
    THROW_ERROR_EXCEPTION("Unsupported cpp_type %v for attribute %Qv",
        static_cast<int>(cppType),
        path);
}

////////////////////////////////////////////////////////////////////////////////

template <bool IsMutableParameter>
class IHandler
{
protected:
    constexpr static bool IsMutable = IsMutableParameter;
    using TGenericMessage = typename TMutabilityTraits<IsMutable>::TGenericMessage;

public:
    virtual ~IHandler() = default;

    //! Handle regular field.
    virtual void HandleRegular(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf path) const = 0;

    //! Handle list (repeated) field item.
    virtual void HandleListItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TParseIndexResult parseIndexResult,
        TStringBuf path) const = 0;

    virtual void HandleListExpansion(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf prefixPath,
        TStringBuf suffixPath) const = 0;

    //! Handle map item.
    virtual void HandleMapItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        const TString& key,
        TStringBuf path) const = 0;

    //! The field is not found.
    virtual void HandleUnknown(TGenericMessage* message, NYPath::TTokenizer& tokenizer) const = 0;

    //! Handle empty intermediate parents.
    //! If returns false, traverse stops, otherwise creates parent initialized with default values.
    virtual bool HandleMissing(TStringBuf path) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <bool IsMutable>
void Traverse(
    typename TMutabilityTraits<IsMutable>::TGenericMessage* root,
    const NYPath::TYPath& path,
    const IHandler<IsMutable>& handler)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();

    while (true) {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto* descriptor = root->GetDescriptor();
        const auto* field = descriptor->FindFieldByName(tokenizer.GetLiteralValue());
        if (!field) {
            handler.HandleUnknown(root, tokenizer);
            break;
        }
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            handler.HandleRegular(root, field, tokenizer.GetPrefix());
            break;
        }
        if (field->is_map()) {
            const auto* mapType = field->message_type();
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            TString key = tokenizer.GetLiteralValue();
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                handler.HandleMapItem(root, field, key, tokenizer.GetPrefix());
                break;
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(
                    mapType->map_value()->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE,
                    "Unexpected type %v for map value at %Qv",
                    mapType->map_value()->type_name(),
                    tokenizer.GetPrefix());

                auto valueIfPresent = LookupMapItemValue<IsMutable>(root, field, key);
                if (!valueIfPresent) {
                    if (handler.HandleMissing(tokenizer.GetPrefix())) {
                        if constexpr (IsMutable) {
                            valueIfPresent.emplace(AddMapItemValue(
                                TMutabilityTraits<IsMutable>::GetMessage(root),
                                field,
                                key));
                            THROW_ERROR_EXCEPTION_UNLESS(*valueIfPresent,
                                "Could not add map item with key %Q", key);
                        } else {
                            THROW_ERROR_EXCEPTION("Cannot add map item to immutable field %Qv",
                                root->GetTypeName());
                        }
                    } else {
                        break;
                    }
                }
                YT_VERIFY(valueIfPresent && *valueIfPresent);
                root = *valueIfPresent;
            }
        } else if (field->is_repeated()) {
            tokenizer.Expect(NYPath::ETokenType::Slash);
            auto prefix = tokenizer.GetPrefix();
            tokenizer.Advance();
            if (tokenizer.GetType() == NYPath::ETokenType::Asterisk) {
                handler.HandleListExpansion(root, field, prefix, tokenizer.GetSuffix());
                break;
            }
            tokenizer.ExpectListIndex();

            TString indexToken = tokenizer.GetLiteralValue();
            auto* reflection = root->GetReflection();
            int count = reflection->FieldSize(*root, field);
            auto parseIndexResult = ParseListIndex(indexToken, count);
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                handler.HandleListItem(root, field, parseIndexResult, tokenizer.GetPrefix());
                break;
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE,
                    "Unexpected type %v of attribute %Qv",
                    field->type_name(),
                    tokenizer.GetPrefixPlusToken());
                if (parseIndexResult.IsOutOfBounds(count)) {
                    THROW_ERROR_EXCEPTION_IF(handler.HandleMissing(tokenizer.GetPrefix()),
                        "Cannot add default values to repeated field %Qv", tokenizer.GetPrefix());
                    break;
                } else {
                    auto index = parseIndexResult.Index;
                    root = TMutabilityTraits<IsMutable>::GetRepeatedMessage(root, field, index);
                }
            }
        } else if (field->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
            const auto* reflection = root->GetReflection();
            if (reflection->HasField(*root, field)) {
                root = TMutabilityTraits<IsMutable>::GetMessage(root, field);
            } else if (handler.HandleMissing(tokenizer.GetPrefix())) {
                THROW_ERROR_EXCEPTION_UNLESS(IsMutable,
                    "Cannot add message %Qv to immutable field %Qv",
                    field->type_name(),
                    root->GetTypeName());
                root = TMutabilityTraits<IsMutable>::GetMessage(root, field);
            } else {
                break;
            }
        } else {
            THROW_ERROR_EXCEPTION("Unexpected type %v of attribute %Qv",
                field->type_name(),
                tokenizer.GetPrefixPlusToken());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TClearHandler
    : public IHandler</*IsMutable*/ true>
{
public:
    explicit TClearHandler(bool skipMissing)
        : SkipMissing_(skipMissing)
    { }

    void HandleRegular(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf path) const final
    {
        const auto* reflection = message->GetReflection();
        // HasField does not work for repeated field, becase there is no difference between empty list and unset list.
        THROW_ERROR_EXCEPTION_UNLESS(SkipMissing_ || field->is_repeated() || reflection->HasField(*message, field),
            "Attribute %Qv is missing",
            path);
        reflection->ClearField(message, field);
    }

    void HandleListItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TParseIndexResult parseIndexResult,
        TStringBuf path) const final
    {
        YT_VERIFY(field->is_repeated());
        const auto* reflection = message->GetReflection();
        int count = reflection->FieldSize(*message, field);
        parseIndexResult.EnsureIndexType(EListIndexType::Absolute, path);
        parseIndexResult.EnsureIndexIsWithinBounds(count, path);
        for (int index = parseIndexResult.Index + 1; index < count; ++index) {
            reflection->SwapElements(message, field, index - 1, index);
        }
        reflection->RemoveLast(message, field);
    }

    void HandleListExpansion(
        TGenericMessage* /*message*/,
        const FieldDescriptor* /*field*/,
        TStringBuf /*prefixPath*/,
        TStringBuf /*suffixPath*/) const final
    {
        THROW_ERROR_EXCEPTION("Clear handler does not support list expansions");
    }

    void HandleMapItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        const TString& key,
        TStringBuf path) const final
    {
        YT_VERIFY(field->is_map());
        TString tmp;
        const auto* reflection = message->GetReflection();
        int count = reflection->FieldSize(*message, field);
        auto item = LookupMapItem<IsMutable>(message, field, key);
        if (!item) {
            THROW_ERROR_EXCEPTION_UNLESS(SkipMissing_, "Attribute %Qv is missing", path);
            return;
        }

        reflection->SwapElements(message, field, item->Index, count - 1);
        reflection->RemoveLast(message, field);
    }

    void HandleUnknown(TGenericMessage* message, NYPath::TTokenizer& tokenizer) const final
    {
        TString key = tokenizer.GetLiteralValue();
        TString value;
        auto* unknownFields = message->GetReflection()->MutableUnknownFields(message);
        auto index = LookupUnknownYsonFieldsItem(unknownFields, key, value);
        if (!index.has_value()) {
            THROW_ERROR_EXCEPTION_UNLESS(SkipMissing_, "Attribute %Qv is missing", tokenizer.GetPrefixPlusToken());
        } else if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            unknownFields->DeleteSubrange(*index, 1);
        } else if (RemoveFromNodeByPath(value, tokenizer)) {
            auto* item = unknownFields->mutable_field(*index)->mutable_length_delimited();
            *item = SerializeUnknownYsonFieldsItem(key, value);
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(SkipMissing_, "Attribute \"%v%v\" is missing",
                tokenizer.GetPrefixPlusToken(),
                tokenizer.GetSuffix());
        }
    }

    bool HandleMissing(TStringBuf path) const final
    {
        THROW_ERROR_EXCEPTION_UNLESS(SkipMissing_, "Attribute %Qv is missing", path);
        return false;
    }

private:
    const bool SkipMissing_;

    static bool RemoveFromNodeByPath(TString& nodeString, NYPath::TTokenizer& tokenizer)
    {
        auto root = ConvertToNode(TYsonStringBuf{nodeString});
        tokenizer.Expect(NYPath::ETokenType::Slash);
        if (RemoveNodeByYPath(root, NYPath::TYPath{tokenizer.GetInput()})) {
            nodeString = ConvertToYsonString(root).ToString();
            return true;
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSetHandler
    : public IHandler</*IsMutable*/ true>
{
public:
    TSetHandler(
        const INodePtr& value,
        const TProtobufWriterOptions& options,
        bool recursive)
        : Value_(value)
        , Options_(options)
        , Recursive_(recursive)
    { }

    void HandleRegular(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf path) const final
    {
        if (field->is_map()) {
            HandleMap(message, field, path);
        } else if (field->is_repeated()) {
            message->GetReflection()->ClearField(message, field);
            AppendValues(message, field, path, Value_->AsList()->GetChildren());
        } else {
            SetValue(message, field, path, Value_);
        }
    }

    void HandleListItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TParseIndexResult parseIndexResult,
        TStringBuf path) const final
    {
        YT_VERIFY(field->is_repeated());
        const auto* reflection = message->GetReflection();
        int count = reflection->FieldSize(*message, field);

        switch (parseIndexResult.IndexType) {
            case EListIndexType::Absolute: {
                parseIndexResult.EnsureIndexIsWithinBounds(count, path);
                SetValue(message, field, parseIndexResult.Index, path, Value_);
                break;
            }
            case EListIndexType::Relative: {
                // Index may be pointing past end of the list
                // as Set with index currently works as `insert before index`.
                parseIndexResult.EnsureIndexIsWithinBounds(count + 1, path);
                int beforeIndex = parseIndexResult.Index;
                AppendValues(message, field, path, {Value_});
                for (int index = beforeIndex; index < count; ++index) {
                    reflection->SwapElements(message, field, index, count);
                }
                break;
            }
        }
    }

    void HandleListExpansion(
        TGenericMessage* /*message*/,
        const FieldDescriptor* /*field*/,
        TStringBuf /*prefixPath*/,
        TStringBuf /*suffixPath*/) const final
    {
        THROW_ERROR_EXCEPTION("Set handler does not support list expansions");
    }

    void HandleMapItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        const TString& key,
        TStringBuf path) const final
    {
        YT_VERIFY(field->is_map());

        auto itemOrError = LookupMapItem<IsMutable>(message, field, key);
        TGenericMessage* item = itemOrError
            ? itemOrError->Message
            : AddMapItem(message, field, key);
        SetValue(item, field->message_type()->map_value(), path, Value_);
    }

    void HandleUnknown(TGenericMessage* message, NYPath::TTokenizer& tokenizer) const final
    {
        switch (Options_.UnknownYsonFieldModeResolver(NYPath::TYPath{tokenizer.GetPrefixPlusToken()})) {
            case EUnknownYsonFieldsMode::Skip:
                return;
            // Forward in a object type handler attribute leaf is interpreted as Fail.
            case EUnknownYsonFieldsMode::Forward:
            case EUnknownYsonFieldsMode::Fail:
                THROW_ERROR_EXCEPTION("Attribute %Qv is unknown", tokenizer.GetPrefixPlusToken());
            case EUnknownYsonFieldsMode::Keep:
                break;
        }
        TString key = tokenizer.GetLiteralValue();
        TString value;
        TString* item = nullptr;
        auto* unknownFields = message->GetReflection()->MutableUnknownFields(message);
        auto index = LookupUnknownYsonFieldsItem(unknownFields, key, value);
        if (!index.has_value()) {
            THROW_ERROR_EXCEPTION_UNLESS(Recursive_ || tokenizer.GetSuffix().empty(),
                "Attribute %Qv is missing",
                tokenizer.GetPrefixPlusToken());
            item = unknownFields->AddLengthDelimited(UnknownYsonFieldNumber);
        } else {
            item = unknownFields->mutable_field(*index)->mutable_length_delimited();
        }

        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            value = ConvertToYsonString(Value_).ToString();
        } else {
            SetNodeValueByPath(value, tokenizer, Value_, Recursive_);
        }
        *item = SerializeUnknownYsonFieldsItem(key, value);
    }

    bool HandleMissing(TStringBuf path) const final
    {
        THROW_ERROR_EXCEPTION_UNLESS(Recursive_, "Attribute %Qv is missing", path);
        return true;
    }

private:
    const INodePtr& Value_;
    const TProtobufWriterOptions& Options_;
    const bool Recursive_;

    void HandleMap(TGenericMessage* message, const FieldDescriptor* field, TStringBuf path) const
    {
        YT_VERIFY(field->is_map());
        const auto* reflection = message->GetReflection();
        reflection->ClearField(message, field);
        for (const auto& [key, value] : Value_->AsMap()->GetChildren()) {
            TString fullPath = Format("%v/%v", path, key);
            auto* item = reflection->AddMessage(message, field);
            SetKey(item, field->message_type()->map_key(), key);
            SetValue(item, field->message_type()->map_value(), fullPath, value);
        }
    }

    void SetValue(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf path,
        const INodePtr& node) const
    {
        const auto* reflection = message->GetReflection();
        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->SetInt32(message, field, ConvertTo<i32>(node));
                return;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->SetInt64(message, field, ConvertTo<i64>(node));
                return;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->SetUInt32(message, field, ConvertTo<ui32>(node));
                return;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->SetUInt64(message, field, ConvertTo<ui64>(node));
                return;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->SetDouble(message, field, ConvertTo<double>(node));
                return;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->SetFloat(message, field, CheckedIntegralCast<float>(ConvertTo<double>(node)));
                return;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->SetBool(message, field, ConvertTo<bool>(node));
                return;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection->SetEnumValue(message, field, ConvertToEnumValue(node, field));
                return;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->SetString(message, field, ConvertTo<TString>(node));
                return;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                DeserializeProtobufMessage(
                    *TMutabilityTraits<IsMutable>::GetMessage(message, field),
                    ReflectProtobufMessageType(field->message_type()),
                    node,
                    GetOptionsByPath(path));
                return;
        }
        ThrowUnsupportedCppType(field->cpp_type(), path);
    }

    void SetValue(
        TGenericMessage* message,
        const FieldDescriptor* field,
        int index,
        TStringBuf path,
        const INodePtr& node) const
    {
        YT_VERIFY(field->is_repeated());
        const auto* reflection = message->GetReflection();
        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->SetRepeatedInt32(message, field, index, ConvertTo<i32>(node));
                return;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->SetRepeatedInt64(message, field, index, ConvertTo<i64>(node));
                return;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->SetRepeatedUInt32(message, field, index, ConvertTo<ui32>(node));
                return;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->SetRepeatedUInt64(message, field, index, ConvertTo<ui64>(node));
                return;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->SetRepeatedDouble(message, field, index, ConvertTo<double>(node));
                return;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->SetRepeatedFloat(message, field, index, CheckedIntegralCast<float>(ConvertTo<double>(node)));
                return;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->SetRepeatedBool(message, field, index, ConvertTo<bool>(node));
                return;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection->SetRepeatedEnumValue(message, field, index, ConvertToEnumValue(node, field));
                return;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->SetRepeatedString(message, field, index, ConvertTo<TString>(node));
                return;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                DeserializeProtobufMessage(
                    *TMutabilityTraits<IsMutable>::GetRepeatedMessage(message, field, index),
                    ReflectProtobufMessageType(field->message_type()),
                    node,
                    GetOptionsByPath(path));
                return;
        }
        ThrowUnsupportedCppType(field->cpp_type(), path);
    }

    void AppendValues(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf path,
        const std::vector<INodePtr>& nodes) const
    {
        YT_VERIFY(field->is_repeated());
        const auto* reflection = message->GetReflection();
        auto doForEach = [=] (auto func) {
            for (const auto& node : nodes) {
                func(node);
            }
        };

        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                doForEach([&] (const INodePtr& node) {
                   reflection->AddInt32(message, field, ConvertTo<i32>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_INT64:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddInt64(message, field, ConvertTo<i64>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_UINT32:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddUInt32(message, field, ConvertTo<ui32>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_UINT64:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddUInt64(message, field, ConvertTo<ui64>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddDouble(message, field, ConvertTo<double>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_FLOAT:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddFloat(message, field, CheckedIntegralCast<float>(ConvertTo<double>(node)));
                });
                return;
            case FieldDescriptor::CPPTYPE_BOOL:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddBool(message, field, ConvertTo<bool>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_ENUM:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddEnumValue(message, field, ConvertToEnumValue(node, field));
                });
                return;
            case FieldDescriptor::CPPTYPE_STRING:
                doForEach([&] (const INodePtr& node) {
                    reflection->AddString(message, field, ConvertTo<TString>(node));
                });
                return;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                doForEach([&, options=GetOptionsByPath(path)] (const INodePtr& node) {
                    DeserializeProtobufMessage(
                        *reflection->AddMessage(message, field),
                        ReflectProtobufMessageType(field->message_type()),
                        node,
                        options);
                });
                return;
        }
        ThrowUnsupportedCppType(field->cpp_type(), path);
    }

    static void SetNodeValueByPath(
        TString& nodeString,
        NYPath::TTokenizer& tokenizer,
        const INodePtr& value,
        bool recursive)
    {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        auto root = !nodeString.empty()
            ? ConvertToNode(TYsonStringBuf{nodeString})
            : GetEphemeralNodeFactory()->CreateMap();

        try {
            SyncYPathSet(
                root,
                NYPath::TYPath{tokenizer.GetInput()},
                ConvertToYsonString(value),
                recursive);
            nodeString = ConvertToYsonString(root).ToString();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Cannot set unknown attribute \"%v%v\"",
                tokenizer.GetPrefixPlusToken(),
                tokenizer.GetSuffix()) << ex;
        }
    }

    TProtobufWriterOptions GetOptionsByPath(TStringBuf basePath) const
    {
        TProtobufWriterOptions options = Options_;
        options.UnknownYsonFieldModeResolver = [=, this] (const NYPath::TYPath& path) {
            return Options_.UnknownYsonFieldModeResolver(basePath + path);
        };
        return options;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TComparisonHandler
    : public IHandler</*IsMutable*/ false>
{
public:
    void HandleRegular(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf /*path*/) const final
    {
        YT_VERIFY(State_.index() == 0);
        State_.emplace<TExistingField>(TExistingField{
            .ParentMessage = message,
            .Field = field
        });
    }

    void HandleListItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TParseIndexResult parseIndexResult,
        TStringBuf path) const final
    {
        YT_VERIFY(State_.index() == 0);
        YT_VERIFY(field->is_repeated());

        parseIndexResult.EnsureIndexType(EListIndexType::Absolute, path);
        auto* reflection = message->GetReflection();
        auto count = reflection->FieldSize(*message, field);
        std::optional<int> indexIfPresent;
        if (!parseIndexResult.IsOutOfBounds(count)) {
            indexIfPresent = parseIndexResult.Index;
        }

        State_.emplace<TRepeatedFieldElement>(TRepeatedFieldElement{
            .ParentMessage = message,
            .Field = field,
            .IndexIfPresent = indexIfPresent
        });
    }

    void HandleListExpansion(
        TGenericMessage* message,
        const FieldDescriptor* field,
        TStringBuf prefixPath,
        TStringBuf suffixPath) const final
    {
        YT_VERIFY(State_.index() == 0);
        YT_VERIFY(field->is_repeated());
        State_.emplace<TRepeatedFieldSubpath>(TRepeatedFieldSubpath{
            .ParentMessage = message,
            .Field = field,
            .Prefix = NYPath::TYPath{prefixPath},
            .Path = NYPath::TYPath{suffixPath},
        });
    }

    void HandleMapItem(
        TGenericMessage* message,
        const FieldDescriptor* field,
        const TString& key,
        TStringBuf /*path*/) const final
    {
        YT_VERIFY(State_.index() == 0);

        auto itemIfPresent = LookupMapItem<IsMutable>(message, field, key);
        std::optional<int> indexIfPresent;
        if (itemIfPresent) {
            indexIfPresent = itemIfPresent->Index;
        }
        State_.emplace<TRepeatedFieldElement>(TRepeatedFieldElement{
            .ParentMessage = message,
            .Field = field,
            .IndexIfPresent = indexIfPresent
        });
    }

    void HandleUnknown(TGenericMessage* /*message*/, NYPath::TTokenizer& tokenizer) const final
    {
        THROW_ERROR_EXCEPTION("Attribute %Qv contains unknown field %Qv", tokenizer.GetPrefix(), tokenizer.GetToken());
    }

    bool HandleMissing(TStringBuf /*path*/) const final
    {
        YT_VERIFY(State_.index() == 0);
        State_.emplace<TMissingField>(TMissingField{});
        return false;
    }

    bool operator==(const TComparisonHandler& rhs) const
    {
        THROW_ERROR_EXCEPTION_IF(State_.index() == 0 || rhs.State_.index() == 0,
            "Protobuf comparison by path failed: traverse result is undefined");
        if (State_.index() != rhs.State_.index()) {
            auto* existing = std::get_if<TExistingField>(&State_);
            if (!existing) {
                existing = std::get_if<TExistingField>(&rhs.State_);
            }
            auto* missing = std::get_if<TMissingField>(&State_);
            if (!missing) {
                missing = std::get_if<TMissingField>(&rhs.State_);
            }

            if (existing && missing) {
                return *existing == *missing;
            } else {
                THROW_ERROR_EXCEPTION("Protobuf comparison by path failed: fields types are of different type");
            }
        }
        return State_ == rhs.State_;
    }

private:
    struct TMissingField
    {
        bool operator==(const TMissingField& /*rhs*/) const
        {
            return true;
        }
    };

    struct TExistingField
    {
        TGenericMessage* ParentMessage;
        const FieldDescriptor* Field;

        bool operator==(const TExistingField& rhs) const
        {
            YT_VERIFY(Field == rhs.Field);
            MessageDifferencer differencer;

            differencer.set_message_field_comparison(MessageDifferencer::MessageFieldComparison::EQUAL);

            std::vector<const FieldDescriptor*> fieldsToCompare{Field};
            return differencer.CompareWithFields(
                *ParentMessage,
                *rhs.ParentMessage,
                fieldsToCompare,
                fieldsToCompare);
        }

        bool operator==(const TMissingField& /*rhs*/) const
        {
            auto* defaultMessage = ParentMessage
                ->GetReflection()
                ->GetMessageFactory()
                ->GetPrototype(ParentMessage->GetDescriptor());
            YT_VERIFY(defaultMessage);
            auto rhs = TExistingField{defaultMessage, Field};
            return rhs == *this;
        }
    };

    struct TRepeatedFieldElement
    {
        TGenericMessage* ParentMessage;
        const FieldDescriptor* Field;
        std::optional<int> IndexIfPresent;

        bool operator==(const TRepeatedFieldElement& rhs) const
        {
            YT_VERIFY(ParentMessage->GetReflection() == rhs.ParentMessage->GetReflection());
            YT_VERIFY(Field == rhs.Field);
            if (IndexIfPresent.has_value() != rhs.IndexIfPresent.has_value()) {
                return false;
            } else if (!IndexIfPresent) {
                return true;
            }
            google::protobuf::util::DefaultFieldComparator comparator;

            auto result = comparator.Compare(
                *ParentMessage,
                *rhs.ParentMessage,
                Field,
                *IndexIfPresent,
                *IndexIfPresent,
                /*fieldContext*/ nullptr);
            switch (result) {
                case google::protobuf::util::FieldComparator::SAME:
                case google::protobuf::util::FieldComparator::DIFFERENT:
                    return result == FieldComparator::SAME;
                case google::protobuf::util::FieldComparator::RECURSE: {
                    MessageDifferencer differencer;

                    return differencer.Equals(
                        *TMutabilityTraits<IsMutable>::GetRepeatedMessage(
                            ParentMessage,
                            Field,
                            *IndexIfPresent),
                        *TMutabilityTraits<IsMutable>::GetRepeatedMessage(
                            rhs.ParentMessage,
                            rhs.Field,
                            *rhs.IndexIfPresent));
                };
                default:
                    YT_ABORT();
            }
        }
    };

    struct TRepeatedFieldSubpath
    {
        TGenericMessage* ParentMessage;
        const FieldDescriptor* Field;
        NYPath::TYPath Prefix;
        NYPath::TYPath Path;

        bool AreRepeatedFieldsEqualAtIndex(
            TGenericMessage& lhsMessage,
            TGenericMessage& rhsMessage,
            const Reflection* reflection,
            const FieldDescriptor* field,
            int index) const
        {
            switch(field->type()) {
                case FieldDescriptor::TYPE_INT32:
                case FieldDescriptor::TYPE_SINT32:
                case FieldDescriptor::TYPE_SFIXED32:
                    return reflection->GetRepeatedInt32(lhsMessage, field, index) == reflection->GetRepeatedInt32(rhsMessage, field, index);
                case FieldDescriptor::TYPE_INT64:
                case FieldDescriptor::TYPE_SINT64:
                case FieldDescriptor::TYPE_SFIXED64:
                    return reflection->GetRepeatedInt64(lhsMessage, field, index) == reflection->GetRepeatedInt64(rhsMessage, field, index);
                case FieldDescriptor::TYPE_UINT32:
                case FieldDescriptor::TYPE_FIXED32:
                    return reflection->GetRepeatedUInt32(lhsMessage, field, index) == reflection->GetRepeatedUInt32(rhsMessage, field, index);
                case FieldDescriptor::TYPE_UINT64:
                case FieldDescriptor::TYPE_FIXED64:
                    return reflection->GetRepeatedUInt64(lhsMessage, field, index) == reflection->GetRepeatedUInt64(rhsMessage, field, index);
                case FieldDescriptor::TYPE_FLOAT:
                    return reflection->GetRepeatedFloat(lhsMessage, field, index) == reflection->GetRepeatedFloat(rhsMessage, field, index);
                case FieldDescriptor::TYPE_DOUBLE:
                    return reflection->GetRepeatedDouble(lhsMessage, field, index) == reflection->GetRepeatedDouble(rhsMessage, field, index);
                case FieldDescriptor::TYPE_BOOL:
                    return reflection->GetRepeatedBool(lhsMessage, field, index) == reflection->GetRepeatedBool(rhsMessage, field, index);
                case FieldDescriptor::TYPE_STRING:
                case FieldDescriptor::TYPE_BYTES: {
                    TProtoStringType s1, s2;
                    return reflection->GetRepeatedStringReference(lhsMessage, field, index, &s1) == reflection->GetRepeatedStringReference(rhsMessage, field, index, &s2);
                }
                case FieldDescriptor::TYPE_ENUM:
                    return reflection->GetRepeatedEnumValue(lhsMessage, field, index) == reflection->GetRepeatedEnumValue(rhsMessage, field, index);
                case FieldDescriptor::TYPE_MESSAGE:
                    return NDetail::AreProtoMessagesEqualByPath(
                        reflection->GetRepeatedMessage(lhsMessage, field, index),
                        reflection->GetRepeatedMessage(rhsMessage, field, index),
                        Path);
                default:
                    return false;
            }
        }

        bool operator==(const TRepeatedFieldSubpath& rhs) const
        {
            YT_VERIFY(Field == rhs.Field);
            YT_VERIFY(Prefix == rhs.Prefix);
            YT_VERIFY(Path == rhs.Path);
            THROW_ERROR_EXCEPTION_UNLESS(Field->type() == FieldDescriptor::TYPE_MESSAGE || Path.empty(),
                "Cannot compare subpaths %Qv of primitive type values at %Qv",
                Prefix,
                Path);
            auto* lhsMessage = ParentMessage;
            auto* rhsMessage = rhs.ParentMessage;
            const auto* reflection = lhsMessage->GetReflection();
            YT_VERIFY(reflection == rhsMessage->GetReflection());

            if (reflection->FieldSize(*lhsMessage, Field) != reflection->FieldSize(*rhsMessage, Field)) {
                return false;
            }

            int size = reflection->FieldSize(*lhsMessage, Field);
            for (int i = 0; i < size; i++) {
                if (!AreRepeatedFieldsEqualAtIndex(*lhsMessage, *rhsMessage, reflection, Field, i)) {
                    return false;
                }
            }

            return true;
        }
    };

    mutable std::variant<
        std::monostate,
        TExistingField,
        TMissingField,
        TRepeatedFieldElement,
        TRepeatedFieldSubpath
    > State_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

bool AreProtoMessagesEqualByPath(
    const google::protobuf::Message& lhs,
    const google::protobuf::Message& rhs,
    const NYPath::TYPath& path)
{
    TComparisonHandler lhsHandler;
    TComparisonHandler rhsHandler;

    THROW_ERROR_EXCEPTION_UNLESS(lhs.GetDescriptor() == rhs.GetDescriptor(),
        "Field %Qv cannot be compared with field %Qv", lhs.GetTypeName(), rhs.GetTypeName());

    Traverse(&lhs, path, lhsHandler);
    Traverse(&rhs, path, rhsHandler);

    return lhsHandler == rhsHandler;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void ClearProtobufFieldByPath(
    google::protobuf::Message& message,
    const NYPath::TYPath& path,
    bool skipMissing)
{
    if (path.empty()) {
        message.Clear();
    } else {
        Traverse(&message, path, TClearHandler{skipMissing});
    }
}

void SetProtobufFieldByPath(
    google::protobuf::Message& message,
    const NYPath::TYPath& path,
    const INodePtr& value,
    const TProtobufWriterOptions& options,
    bool recursive)
{
    if (path.empty()) {
        DeserializeProtobufMessage(
            message,
            ReflectProtobufMessageType(message.GetDescriptor()),
            value,
            options);
    } else {
        Traverse(&message, path, TSetHandler{value, options, recursive});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
