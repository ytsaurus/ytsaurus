#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
#endif
#undef SERIALIZE_INL_H_

#include "../misc/nullable.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T ParseYson(const TYson& yson)
{
    auto node = DeserializeFromYson(yson);
    T value;
    Read(value, ~node);
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
inline void Read(
    TIntrusivePtr<T>& parameter,
    const NYTree::INode* node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType)
{
    if (!parameter) {
        parameter = New<T>();
    }
    parameter->Load(node);
}

// TEnumBase
template <class T>
inline void Read(
    T& parameter,
    const NYTree::INode* node, 
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType)
{
    auto value = node->AsString()->GetValue();
    parameter = ParseEnum<T>(value);
}

// TNullable
template <class T>
inline void Read(TNullable<T>& parameter, const NYTree::INode* node)
{
    T value;
    Read(value, node);
    parameter = value;
}

// yvector
template <class T>
inline void Read(yvector<T>& parameter, const NYTree::INode* node)
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
inline void Read(yhash_set<T>& parameter, const NYTree::INode* node)
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
inline void Read(yhash_map<Stroka, T>& parameter, const NYTree::INode* node)
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

}
}