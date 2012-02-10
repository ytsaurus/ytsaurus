#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
#endif
#undef SERIALIZE_INL_H_

#include "ytree.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(const TYson& yson)
{
    typedef typename TDeserializeTraits<T>::TReturnType TResult;
    auto node = DeserializeFromYson(yson, GetEphemeralNodeFactory());
    TResult value;
    Read(value, ~node);
    return value;
}

template <class T>
TYson SetializeToYson(
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
    const INode* node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TConfigurable*>, int>::TType)
{
    if (!parameter) {
        parameter = New<T>();
    }
    // static_cast is needed because T can override method Load
    // without default value for parameter path
    static_cast<TConfigurable*>(~parameter)->Load(node);
}

// TEnumBase
template <class T>
void Read(
    T& parameter,
    const INode* node, 
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType)
{
    auto value = node->AsString()->GetValue();
    parameter = ParseEnum<T>(value);
}

// TNullable
template <class T>
void Read(TNullable<T>& parameter, const INode* node)
{
    T value;
    Read(value, node);
    parameter = value;
}

// yvector
template <class T>
void Read(yvector<T>& parameter, const INode* node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    parameter.resize(size);
    for (int i = 0; i < size; ++i) {
        Read(parameter[i], ~listNode->GetChild(i));
    }
}

// yhash_set
template <class T>
void Read(yhash_set<T>& parameter, const INode* node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    for (int i = 0; i < size; ++i) {
        T value;
        Read(value, ~listNode->GetChild(i));
        parameter.insert(MoveRV(value));
    }
}

// yhash_map
template <class T>
void Read(yhash_map<Stroka, T>& parameter, const INode* node)
{
    auto mapNode = node->AsMap();
    FOREACH (const auto& pair, mapNode->GetChildren()) {
        auto& key = pair.first;
        T value;
        Read(value, ~pair.second);
        parameter.insert(MakePair(key, MoveRV(value)));
    }
}

////////////////////////////////////////////////////////////////////////////////

// TConfigurable::TPtr
template <class T>
void Write(
    const TIntrusivePtr<T>& parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType)
{
    YASSERT(parameter);
    parameter->Save(consumer);
}

// TEnumBase
template <class T>
void Write(
    const T& parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType)
{
    consumer->OnStringScalar(parameter.ToString());
}

// TNullable
template <class T>
void Write(const TNullable<T>& parameter, IYsonConsumer* consumer)
{
    YASSERT(parameter);
    Write(*parameter, consumer);
}

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
        consumer->OnMapItem(pair->first);
        Write(pair->second, consumer);
    }
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

}
}
