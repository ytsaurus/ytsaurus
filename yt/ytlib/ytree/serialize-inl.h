#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
#endif
#undef SERIALIZE_INL_H_

#include "ytree.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(const TYson& yson)
{
    auto node = DeserializeFromYson(yson, GetEphemeralNodeFactory());
    return DeserializeFromYson<T>(node);
}

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(const TYson& yson, const TYPath& path)
{
    auto node = DeserializeFromYson(yson);
    return DeserializeFromYson<T>(node, path);
}

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(INodePtr node)
{
    typedef typename TDeserializeTraits<T>::TReturnType TResult;
    TResult value;
    Read(value, node);
    return value;
}

INodePtr GetNodeByYPath(INodePtr root, const TYPath& path);

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(INodePtr node, const TYPath& path)
{
    auto subnode = GetNodeByYPath(node, path);
    return DeserializeFromYson<T>(subnode);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TYson SerializeToYson(
    const T& value,
    EYsonFormat format)
{
    TStringStream output;
    TYsonWriter writer(&output, format);
    Write(value, &writer);
    return output.Str();
}

template <class T>
T CheckedStaticCast(i64 value)
{
    if (value < Min<T>() || value > Max<T>()) {
        ythrow yexception()
            << Sprintf("Argument is out of integral range (Value: %" PRId64 ")", value);
    }
    return static_cast<T>(value);
}

// TConfigurable::TPtr
template <class T>
void Read(
    TIntrusivePtr<T>& parameter,
    INodePtr node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TConfigurable*>, int>::TType)
{
    if (!parameter) {
        parameter = New<T>();
    }
    // static_cast is needed because T can override method Load
    // without default value for parameter path
    static_cast<TConfigurable*>(~parameter)->Load(node, false);
}

// TEnumBase
template <class T>
void Read(
    T& parameter,
    INodePtr node, 
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType)
{
    auto value = node->AsString()->GetValue();
    parameter = ParseEnum<T>(value);
}

// TNullable
template <class T>
void Read(TNullable<T>& parameter, INodePtr node)
{
    T value;
    Read(value, node);
    parameter = value;
}

// yvector
template <class T>
void Read(yvector<T>& parameter, INodePtr node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    parameter.resize(size);
    for (int i = 0; i < size; ++i) {
        Read(parameter[i], listNode->GetChild(i));
    }
}

// yhash_set
template <class T>
void Read(yhash_set<T>& parameter, INodePtr node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    for (int i = 0; i < size; ++i) {
        T value;
        Read(value, listNode->GetChild(i));
        parameter.insert(MoveRV(value));
    }
}

// yhash_map
template <class T>
void Read(yhash_map<Stroka, T>& parameter, INodePtr node)
{
    auto mapNode = node->AsMap();
    FOREACH (const auto& pair, mapNode->GetChildren()) {
        auto& key = pair.first;
        T value;
        Read(value, pair.second);
        parameter.insert(MakePair(key, MoveRV(value)));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Write(T* parameter, IYsonConsumer* consumer)
{
    YASSERT(parameter);
    Write(*parameter, consumer);
}

template <class T>
void Write(const TIntrusivePtr<T>& parameter, IYsonConsumer* consumer)
{
    YASSERT(parameter);
    Write(*parameter, consumer);
}

// TConfigurable::TPtr
template <class T>
void Write(
    const T& parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TConfigurable*>, int>::TType)
{
    parameter.Save(consumer);
}

// TEnumBase
template <class T>
void Write(
    T parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType)
{
    consumer->OnStringScalar(FormatEnum(parameter));
}

// TNullable
template <class T>
void Write(const TNullable<T>& parameter, IYsonConsumer* consumer)
{
    YASSERT(parameter);
    Write(*parameter, consumer);
}

// TODO(panin): kill this once we get rid of yvector
// yvector
template <class T>
void Write(const yvector<T>& parameter, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    FOREACH (const auto& value, parameter) {
        consumer->OnListItem();
        Write(value, consumer);
    }
    consumer->OnEndList();
}

// std::vector
template <class T>
void Write(const std::vector<T>& parameter, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    FOREACH (const auto& value, parameter) {
        consumer->OnListItem();
        Write(value, consumer);
    }
    consumer->OnEndList();
}

// yhash_set
template <class T>
void Write(const yhash_set<T>& parameter, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    auto sortedItems = GetSortedIterators(parameter);
    FOREACH (const auto& value, sortedItems) {
        consumer->OnListItem();
        Write(*value, consumer);
    }
    consumer->OnEndList();
}

// yhash_map
template <class T>
void Write(const yhash_map<Stroka, T>& parameter, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    auto sortedItems = GetSortedIterators(parameter);
    FOREACH (const auto& pair, sortedItems) {
        consumer->OnKeyedItem(pair->first);
        Write(pair->second, consumer);
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
