#ifndef ATTRIBUTE_SCHEMA_TRAITS_H_
#error "Direct inclusion of this file is not allowed, include scalar_attribute_traits.h"
// For the sake of sane code completion.
#include "attribute_schema_traits.h"
#endif

#include "attribute_schema.h"

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TScalarAttributePathResolver
{
    EValueType ValidateAndExtractType(
        const TAttributeSchema* attribute,
        NYPath::TYPathBuf path)
    {
        if (!path.empty()) {
            THROW_ERROR_EXCEPTION_UNLESS(TTypeToValueType<T>::ValueType == EValueType::Any,
                "Attribute %Qv is scalar and does not support nested access",
                attribute->FormatPathEtc());
            return EValueType::Any;
        }

        return TTypeToValueType<T>::ValueType;
    }
};

template <class T, bool IsList>
struct TScalarAttributeContainerPathResolver
    : public TScalarAttributePathResolver<T>
{
    EValueType ValidateAndExtractType(
        const TAttributeSchema* attribute,
        NYPath::TYPathBuf path)
    {
        if (path.Empty()) {
            return EValueType::Any;
        }

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();

        if constexpr (IsList) {
            tokenizer.ExpectListIndex();
        } else {
            tokenizer.Expect(NYPath::ETokenType::Literal);
        }

        tokenizer.Advance();
        return TScalarAttributePathResolver<T>::ValidateAndExtractType(
            attribute,
            tokenizer.GetInput());
    }
};

template <class T>
struct TScalarAttributePathResolver<std::vector<T>>
    : public TScalarAttributeContainerPathResolver<T, /*IsList*/ true>
{ };

template <class K, class V>
struct TScalarAttributePathResolver<THashMap<K, V>>
    : public TScalarAttributeContainerPathResolver<V, /*IsList*/ false>
{ };

template <class T>
    requires std::convertible_to<T*, ::google::protobuf::MessageLite*>
struct TScalarAttributePathResolver<T>
{
    EValueType ValidateAndExtractType(
        const TAttributeSchema* attribute,
        NYPath::TYPathBuf path)
    {
        const auto* protobufType = NYson::ReflectProtobufMessageType<T>();
        try {
            NYson::TResolveProtobufElementByYPathOptions options;
            if (attribute->IsExtensible(path)) {
                options.AllowUnknownYsonFields = true;
            }

            auto element = NYson::ResolveProtobufElementByYPath(protobufType, path, options).Element;
            if (auto* scalarElement = std::get_if<std::unique_ptr<NYson::TProtobufScalarElement>>(&element)) {
                auto type = scalarElement->get()->Type;
                auto enumStorageType = scalarElement->get()->EnumStorageType;
                return ProtobufToTableValueType(type, enumStorageType);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error fetching field %Qv of attribute %Qv",
                path,
                attribute->FormatPathEtc())
                << ex;
        }

        return EValueType::Any;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue, bool ForceSetViaYson = false>
struct TScalarAttributeUpdateTraits
{
    static void UpdateValue(
        const TAttributeSchema* schema,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& newValue,
        TTypedValue& mutableValue,
        bool recursive)
    {
        if (path.Empty()) {
            mutableValue = ParseScalarAttributeFromYsonNode<TTypedValue>(schema, newValue);
        } else {
            auto nodeValue = NYTree::ConvertToNode(mutableValue);
            NYTree::SyncYPathSet(nodeValue, path, NYson::ConvertToYsonString(newValue), recursive);
            mutableValue = ParseScalarAttributeFromYsonNode<TTypedValue>(schema, nodeValue);
        }
    }
};

template <class TTypedValue>
    requires std::is_convertible_v<TTypedValue*, google::protobuf::Message*>
struct TScalarAttributeUpdateTraits<TTypedValue, /*ForceSetViaYson*/ false>
{
    static void UpdateValue(
        const TAttributeSchema* schema,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& newValue,
        TTypedValue& mutableValue,
        bool recursive)
    {
        NYson::TProtobufWriterOptions options;
        options.UnknownYsonFieldModeResolver = schema->BuildUnknownYsonFieldModeResolver();
        NAttributes::SetProtobufFieldByPath(mutableValue, path, newValue, options, recursive);
    }
};

template <class TValue>
struct TScalarAttributeUpdateTraits<std::vector<TValue>, /*ForceSetViaYson*/ false>
{
    static void UpdateValue(
        const TAttributeSchema* schema,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& newValue,
        std::vector<TValue>& mutableValue,
        bool recursive)
    {
        if (path.Empty()) {
            mutableValue.clear();
            if (newValue->GetType() == NYTree::ENodeType::Entity) {
                return;
            }

            auto children = newValue->AsList()->GetChildren();
            mutableValue.resize(children.size());
            for (int i = 0; i < std::ssize(children); ++i) {
                TScalarAttributeUpdateTraits<TValue>::UpdateValue(
                    schema,
                    NYPath::TYPath{},
                    children[i],
                    mutableValue[i],
                    recursive);
            }

            return;
        }

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Expect(NYPath::ETokenType::StartOfStream);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.ExpectListIndex();

        auto token = tokenizer.GetToken();
        bool pathEndReached = tokenizer.Advance() == NYPath::ETokenType::EndOfStream;
        auto pathSuffix = NYPath::TYPath{tokenizer.GetInput()};
        auto index = NAttributes::ParseListIndex(token, mutableValue.size());

        switch (index.IndexType) {
            case NAttributes::EListIndexType::Absolute: {
                index.EnsureIndexIsWithinBounds(mutableValue.size(), path);
                TScalarAttributeUpdateTraits<TValue>::UpdateValue(
                    schema,
                    pathSuffix,
                    newValue,
                    mutableValue[index.Index],
                    recursive);
                break;
            }
            case NAttributes::EListIndexType::Relative: {
                index.EnsureIndexIsWithinBounds(mutableValue.size() + 1, path);
                THROW_ERROR_EXCEPTION_UNLESS(
                    pathEndReached,
                    "Error setting value at %v: encountered %Qv token is allowed in the end of path only",
                    path,
                    token);
                mutableValue.emplace_back();
                std::move_backward(mutableValue.begin() + index.Index, mutableValue.end() - 1, mutableValue.end());
                TScalarAttributeUpdateTraits<TValue>::UpdateValue(
                    schema,
                    NYPath::TYPath{},
                    newValue,
                    mutableValue[index.Index],
                    recursive);
                break;
            }
        }
    }
};

template <class TKey, class TValue>
struct TScalarAttributeUpdateTraits<THashMap<TKey, TValue>, /*ForceSetViaYson*/ false>
{
    static void UpdateValue(
        const TAttributeSchema* schema,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& newValue,
        THashMap<TKey, TValue>& mutableValue,
        bool recursive)
    {
        if (path.Empty()) {
            mutableValue.clear();
            if (newValue->GetType() == NYTree::ENodeType::Entity) {
                return;
            }

            auto mapNode = newValue->AsMap();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                TScalarAttributeUpdateTraits<TValue>::UpdateValue(
                    schema,
                    NYPath::TYPath{},
                    child,
                    mutableValue[FromString<TKey>(key)],
                    recursive);
            }
            return;
        }

        NYPath::TTokenizer tokenizer(path);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto key = FromString<TKey>(tokenizer.GetLiteralValue());
        tokenizer.Advance();

        TScalarAttributeUpdateTraits<TValue>::UpdateValue(
            schema,
            NYPath::TYPath{tokenizer.GetInput()},
            newValue,
            mutableValue[key],
            recursive);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
void UpdateScalarAttributeValue(
    const TAttributeSchema* schema,
    const NYPath::TYPath& path,
    const NYTree::INodePtr& newValue,
    TTypedValue& mutableValue,
    bool recursive,
    bool forceSetViaYson)
{
    if (forceSetViaYson) {
        NDetail::TScalarAttributeUpdateTraits<TTypedValue, /*ForceSetViaYson*/ true>::UpdateValue(
            schema,
            path,
            newValue,
            mutableValue,
            recursive);
    } else {
        NDetail::TScalarAttributeUpdateTraits<TTypedValue, /*ForceSetViaYson*/ false>::UpdateValue(
            schema,
            path,
            newValue,
            mutableValue,
            recursive);
    }
}

template <class TTypedValue>
NTableClient::EValueType ValidateAndExtractTypeFromScalarAttribute(
    const TAttributeSchema* attribute,
    NYPath::TYPathBuf path)
{
    NDetail::TScalarAttributePathResolver<TTypedValue> resolver;
    return resolver.ValidateAndExtractType(attribute, path);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires std::convertible_to<T*, ::google::protobuf::MessageLite*>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema* schema, const NYTree::INodePtr& node, const T*)
{
    NYson::TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = schema->BuildUnknownYsonFieldModeResolver();
    T message;
    NYTree::DeserializeProtobufMessage(
        message,
        NYson::ReflectProtobufMessageType<T>(),
        node,
        options);
    return message;
}

template <class T>
    requires NMpl::IsSpecialization<T, std::vector>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema* schema, const NYTree::INodePtr& node, const T*)
{
    T result;
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    result.reserve(size);
    for (int i = 0; i < size; ++i) {
        result.push_back(ParseScalarAttributeFromYsonNode<typename T::value_type>(
            schema,
            listNode->GetChildOrThrow(i)));
    }
    return result;
}

template <class T>
    requires NMpl::IsSpecialization<T, THashMap>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema* schema, const NYTree::INodePtr& node, const T*)
{
    T result;
    auto mapNode = node->AsMap();
    auto size = mapNode->GetChildCount();
    result.reserve(size);
    for (const auto& [serializedKey, serializedItem] : mapNode->GetChildren()) {
        typename T::key_type key;
        NYTree::NDetail::TMapKeyHelper<typename T::key_type>::Deserialize(key, serializedKey);
        result.emplace(
            std::move(key),
            ParseScalarAttributeFromYsonNode<typename T::mapped_type>(schema, serializedItem));
    }
    return result;
}

template <class T>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema*, const NYTree::INodePtr& node, const T*)
{
    return NYTree::ConvertTo<T>(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NSever::NObjects
