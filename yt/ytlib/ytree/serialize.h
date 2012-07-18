#pragma once

#include "ephemeral.h"
#include "yson_writer.h"
#include "yson_producer.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/mpl.h>
#include <ytlib/misc/guid.h>

namespace NYT {
    class TYsonSerializable;
}

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
EYsonType GetYsonType(const T&);
EYsonType GetYsonType(const TYsonString& yson);
EYsonType GetYsonType(const TYsonInput& input);
EYsonType GetYsonType(const TYsonProducer& producer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void WriteYson(
    TOutputStream* output,
    const T& value,
    EYsonType type,
    EYsonFormat format = EYsonFormat::Binary);

template <class T>
void WriteYson(
    TOutputStream* output,
    const T& value,
    EYsonFormat format = EYsonFormat::Binary);

template <class T>
void WriteYson(
    const TYsonOutput& output,
    const T& value,
    EYsonFormat format = EYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Serialize(T* value, IYsonConsumer* consumer);

template <class T>
void Serialize(const TIntrusivePtr<T>& value, IYsonConsumer* consumer);

// size_t
void Serialize(size_t value, IYsonConsumer* consumer);

// i64
void Serialize(i64 value, IYsonConsumer* consumer);

// i32
void Serialize(i32 value, IYsonConsumer* consumer);

// ui32
void Serialize(ui32 value, IYsonConsumer* consumer);

// ui16
void Serialize(ui16 value, IYsonConsumer* consumer);

// double
void Serialize(double value, IYsonConsumer* consumer);

// Stroka
void Serialize(const Stroka& value, IYsonConsumer* consumer);

// TStringBuf
void Serialize(const TStringBuf& value, IYsonConsumer* consumer);

// const char*
void Serialize(const char* value, IYsonConsumer* consumer);

// bool
void Serialize(bool value, IYsonConsumer* consumer);

// char
void Serialize(char value, IYsonConsumer* consumer);

// TDuration
void Serialize(TDuration value, IYsonConsumer* consumer);

// TInstant
void Serialize(TInstant value, IYsonConsumer* consumer);

// TGuid
void Serialize(const TGuid& value, IYsonConsumer* consumer);

// TInputStream
void Serialize(TInputStream& input, IYsonConsumer* consumer);

// TEnumBase
template <class T>
void Serialize(
    T value,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TEnumBase<T>&>, int>::TType = 0);

// TNullable
template <class T>
void Serialize(const TNullable<T>& value, IYsonConsumer* consumer);

// TODO(roizner): move to ytree.h
void Serialize(INode& value, IYsonConsumer* consumer);

// std::vector
template <class T>
void Serialize(const std::vector<T>& value, IYsonConsumer* consumer);

// std::vector
template <class T>
void Serialize(const std::vector<T>& value, IYsonConsumer* consumer);

// yhash_set
template <class T>
void Serialize(const yhash_set<T>& value, IYsonConsumer* consumer);

// yhash_map
template <class T>
void Serialize(const yhash_map<Stroka, T>& value, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Deserialize(TIntrusivePtr<T>& value, INodePtr node);

template <class T>
void Deserialize(TAutoPtr<T>& value, INodePtr node);

// i64
void Deserialize(i64& value, INodePtr node);

// i32
void Deserialize(i32& value, INodePtr node);

// ui32
void Deserialize(ui32& value, INodePtr node);

// ui16
void Deserialize(ui16& value, INodePtr node);

// double
void Deserialize(double& value, INodePtr node);

// Stroka
void Deserialize(Stroka& value, INodePtr node);

// bool
void Deserialize(bool& value, INodePtr node);

// char
void Deserialize(char& value, INodePtr node);

// TDuration
void Deserialize(TDuration& value, INodePtr node);

// TInstant
void Deserialize(TInstant& value, INodePtr node);

// TGuid
void Deserialize(TGuid& value, INodePtr node);

// TEnumBase
template <class T>
void Deserialize(
    T& value,
    INodePtr node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, TEnumBase<T>&>, int>::TType = 0);

// TNullable
template <class T>
void Deserialize(TNullable<T>& value, INodePtr node);

// INode
void Deserialize(
    INodePtr& value,
    INodePtr node);

// std::vector
template <class T>
void Deserialize(std::vector<T>& value, INodePtr node);

// std::vector
template <class T>
void Deserialize(std::vector<T>& value, INodePtr node);

// yhash_set
template <class T>
void Deserialize(yhash_set<T>& value, INodePtr node);

// yhash_map
template <class T>
void Deserialize(yhash_map<Stroka, T>& value, INodePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
