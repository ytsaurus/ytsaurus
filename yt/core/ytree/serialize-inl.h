#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
#endif

#include "node.h"
#include "yson_serializable.h"

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/collection_helpers.h>

#include <yt/core/yson/stream.h>
#include <yt/core/yson/string.h>

#include <numeric>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void SerializeVector(const T& items, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (const auto& item : items) {
        consumer->OnListItem();
        Serialize(item, consumer);
    }
    consumer->OnEndList();
}

template <class T>
void SerializeSet(const T& items, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (auto it : GetSortedIterators(items)) {
        consumer->OnListItem();
        Serialize(*it, consumer);
    }
    consumer->OnEndList();
}

template <class T>
void SerializeMap(const T& items, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    for (auto it : GetSortedIterators(items)) {
        consumer->OnKeyedItem(ToString(it->first));
        Serialize(it->second, consumer);
    }
    consumer->OnEndMap();
}

template <class T>
void DeserializeVector(T& value, INodePtr node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    value.resize(size);
    for (int i = 0; i < size; ++i) {
        Deserialize(value[i], listNode->GetChild(i));
    }
}

template <class T>
void DeserializeSet(T& value, INodePtr node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    for (int i = 0; i < size; ++i) {
        typename T::value_type value;
        Deserialize(value, listNode->GetChild(i));
        value.insert(std::move(value));
    }
}

template <class T>
void DeserializeMap(T& value, INodePtr node)
{
    auto mapNode = node->AsMap();
    value.clear();
    for (const auto& pair : mapNode->GetChildren()) {
        auto key = FromString<typename T::key_type>(pair.first);
        typename T::mapped_type item;
        Deserialize(item, pair.second);
        value.insert(std::make_pair(std::move(key), std::move(item)));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::EYsonType GetYsonType(const T&)
{
    return NYson::EYsonType::Node;
}

template <class T>
void WriteYson(
    TOutputStream* output,
    const T& value,
    NYson::EYsonType type,
    NYson::EYsonFormat format,
    int indent)
{
    NYson::TYsonWriter writer(output, format, type, false, false, indent);
    Serialize(value, &writer);
}

template <class T>
void WriteYson(
    TOutputStream* output,
    const T& value,
    NYson::EYsonFormat format)
{
    WriteYson(output, value, GetYsonType(value), format);
}

template <class T>
void WriteYson(
    const NYson::TYsonOutput& output,
    const T& value,
    NYson::EYsonFormat format)
{
    WriteYson(output.GetStream(), value, output.GetType(), format);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Serialize(T* value, NYson::IYsonConsumer* consumer)
{
    YASSERT(value);
    Serialize(*value, consumer);
}

template <class T>
void Serialize(const TIntrusivePtr<T>& value, NYson::IYsonConsumer* consumer)
{
    Serialize(value.Get(), consumer);
}

// Enums
template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type Serialize(
    T value,
    NYson::IYsonConsumer* consumer)
{
    consumer->OnStringScalar(FormatEnum(value));
}

// TNullable
template <class T>
void Serialize(const TNullable<T>& value, NYson::IYsonConsumer* consumer)
{
    if (!value) {
        consumer->OnEntity();
    } else {
        Serialize(*value, consumer);
    }
}

// std::vector
template <class T>
void Serialize(const std::vector<T>& items, NYson::IYsonConsumer* consumer)
{
    SerializeVector(items, consumer);
}

// SmallVector
template <class T, unsigned N>
void Serialize(const SmallVector<T, N>& items, NYson::IYsonConsumer* consumer)
{
    SerializeVector(items, consumer);
}

// std::set
template <class T>
void Serialize(const std::set<T>& items, NYson::IYsonConsumer* consumer)
{
    SerializeSet(items, consumer);
}

// yhash_set
template <class T>
void Serialize(const yhash_set<T>& items, NYson::IYsonConsumer* consumer)
{
    SerializeSet(items, consumer);
}

// std::map
template <class K, class V>
void Serialize(const std::map<K, V>& items, NYson::IYsonConsumer* consumer)
{
    SerializeMap(items, consumer);
}

// yhash_map
template <class K, class V>
void Serialize(const yhash_map<K, V>& items, NYson::IYsonConsumer* consumer)
{
    SerializeMap(items, consumer);
}

// TErrorOr
template <class T>
void Serialize(const TErrorOr<T>& error, NYson::IYsonConsumer* consumer)
{
    const TError& justError = error;
    if (error.IsOK()) {
        std::function<void(NYson::IYsonConsumer*)> valueProducer = [&error] (NYson::IYsonConsumer* consumer) {
            Serialize(error.Value(), consumer);
        };
        Serialize(justError, consumer, &valueProducer);
    } else {
        Serialize(justError, consumer);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Deserialize(TIntrusivePtr<T>& value, INodePtr node)
{
    if (!value) {
        value = New<T>();
    }
    Deserialize(*value, node);
}

template <class T>
void Deserialize(std::unique_ptr<T>& value, INodePtr node)
{
    if (!value) {
        value.Reset(new T());
    }
    Deserialize(*value, node);
}

template <class T, class S>
T CheckedStaticCast(S value)
{
    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        THROW_ERROR_EXCEPTION("Argument value %v is out of expected range",
            value);
    }
    return static_cast<T>(value);
}

// Enums
template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type Deserialize(
    T& value,
    INodePtr node)
{
    auto stringValue = node->AsString()->GetValue();
    value = ParseEnum<T>(stringValue);
}

// TNullable
template <class T>
void Deserialize(TNullable<T>& value, INodePtr node)
{
    if (node->GetType() == ENodeType::Entity) {
        value = Null;
    } else {
        if (!value) {
            value = T();
        }
        Deserialize(*value, node);
    }
}

// std::vector
template <class T>
void Deserialize(std::vector<T>& value, INodePtr node)
{
    DeserializeVector(value, node);
}

// SmallVector
template <class T, unsigned N>
void Deserialize(SmallVector<T, N>& value, INodePtr node)
{
    DeserializeVector(value, node);
}

// std::set
template <class T>
void Deserialize(std::set<T>& value, INodePtr node)
{
    DeserializeSet(value, node);
}

// yhash_set
template <class T>
void Deserialize(yhash_set<T>& value, INodePtr node)
{
    DeserializeSet(value, node);
}

// std::map
template <class K, class V>
void Deserialize(std::map<K, V>& value, INodePtr node)
{
    DeserializeMap(value, node);
}

// yhash_map
template <class K, class V>
void Deserialize(yhash_map<K, V>& value, INodePtr node)
{
    DeserializeMap(value, node);
}

// TErrorOr
template <class T>
void Deserialize(TErrorOr<T>& error, NYTree::INodePtr node)
{
    TError& justError = error;
    Deserialize(justError, node);
    if (error.IsOK()) {
        auto mapNode = node->AsMap();
        auto valueNode = mapNode->FindChild("value");
        if (valueNode) {
            Deserialize(error.Value(), std::move(valueNode));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
