#pragma once

#include "ephemeral.h"
#include "yson_writer.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/mpl.h>
#include <ytlib/misc/guid.h>

namespace NYT {
    class TConfigurable;
}

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T, class>
struct TDeserializeTraits
{
    typedef T TReturnType;
};

template <class T>
struct TDeserializeTraits<
    T, 
    typename NMpl::TEnableIf< NMpl::TIsConvertible<T*, TRefCounted*> >::TType
>
{
    typedef TIntrusivePtr<T> TReturnType;
};

////////////////////////////////////////////////////////////////////////////////

INodePtr CloneNode(
    INodePtr node,
    INodeFactory* factory = GetEphemeralNodeFactory());

TYsonProducer ProducerFromYson(TInputStream* input, EYsonType type = EYsonType::Node);

TYsonProducer ProducerFromYson(const TYson& data, EYsonType type = EYsonType::Node);

TYsonProducer ProducerFromNode(INodePtr node);

//! Checks YSON stream for correctness, throws exception on error.
void ValidateYson(TInputStream* input);

//! Checks YSON string for correctness, throws exception on error.
void ValidateYson(const TYson& yson);

////////////////////////////////////////////////////////////////////////////////

INodePtr DeserializeFromYson(
    TInputStream* input,
    INodeFactory* factory = GetEphemeralNodeFactory());

INodePtr DeserializeFromYson(
    const TYson& yson,
    INodeFactory* factory = GetEphemeralNodeFactory());

INodePtr DeserializeFromYson(
    const TStringBuf& yson,
    INodeFactory* factory = GetEphemeralNodeFactory());

INodePtr DeserializeFromYson(
    TYsonProducer producer,
    INodeFactory* factory = GetEphemeralNodeFactory());

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(const TYson& yson);

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(const TYson& yson, const TYPath& path);

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(INodePtr node);

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(INodePtr node, const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

TOutputStream& SerializeToYson(
    INodePtr node,
    TOutputStream& output,
    EYsonFormat format = EYsonFormat::Binary);

TYson SerializeToYson(
    TYsonProducer producer,
    EYsonFormat format = EYsonFormat::Binary);

template <class T>
TYson SerializeToYson(
    const T& value,
    EYsonFormat format = EYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

// TConfigurable::TPtr
template <class T>
void Read(
    TIntrusivePtr<T>& parameter,
    INodePtr node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TConfigurable*>, int>::TType = 0);

// i64
void Read(i64& parameter, INodePtr node);

// i32
void Read(i32& parameter, INodePtr node);

// ui32
void Read(ui32& parameter, INodePtr node);

// ui16
void Read(ui16& parameter, INodePtr node);

// double
void Read(double& parameter, INodePtr node);

// Stroka
void Read(Stroka& parameter, INodePtr node);

// bool
void Read(bool& parameter, INodePtr node);

// char
void Read(char& parameter, INodePtr node);

// TDuration
void Read(TDuration& parameter, INodePtr node);

// TInstant
void Read(TInstant& parameter, INodePtr node);

// TGuid
void Read(TGuid& parameter, INodePtr node);

// TEnumBase
template <class T>
void Read(
    T& parameter,
    INodePtr node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType = 0);

// TNullable
template <class T>
void Read(TNullable<T>& parameter, INodePtr node);

// TNodePtr
void Read(
    INodePtr& parameter,
    INodePtr node);

// yvector
template <class T>
void Read(yvector<T>& parameter, INodePtr node);

// std::vector
template <class T>
void Read(std::vector<T>& parameter, INodePtr node);

// yhash_set
template <class T>
void Read(yhash_set<T>& parameter, INodePtr node);

// yhash_map
template <class T>
void Read(yhash_map<Stroka, T>& parameter, INodePtr node);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Write(T* parameter, IYsonConsumer* consumer);

template <class T>
void Write(const TIntrusivePtr<T>& parameter, IYsonConsumer* consumer);

// TConfigurable::TPtr
template <class T>
void Write(
    const T& parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TConfigurable*>, int>::TType = 0);

// i64
void Write(i64 parameter, IYsonConsumer* consumer);

// i32
void Write(i32 parameter, IYsonConsumer* consumer);

// ui32
void Write(ui32 parameter, IYsonConsumer* consumer);

// ui16
void Write(ui16 parameter, IYsonConsumer* consumer);

// double
void Write(double parameter, IYsonConsumer* consumer);

// Stroka
void Write(const Stroka& parameter, IYsonConsumer* consumer);

// bool
void Write(bool parameter, IYsonConsumer* consumer);

// char
void Write(char parameter, IYsonConsumer* consumer);

// TDuration
void Write(TDuration parameter, IYsonConsumer* consumer);

// TInstant
void Write(TInstant parameter, IYsonConsumer* consumer);

// TGuid
void Write(const TGuid& parameter, IYsonConsumer* consumer);

// TEnumBase
template <class T>
void Write(
    T parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType = 0);

// TNullable
template <class T>
void Write(const TNullable<T>& parameter, IYsonConsumer* consumer);

// TNodePtr
void Write(INode& parameter, IYsonConsumer* consumer);

// yvector
template <class T>
void Write(const yvector<T>& parameter, IYsonConsumer* consumer);

// std::vector
template <class T>
void Write(const std::vector<T>& parameter, IYsonConsumer* consumer);

// yhash_set
template <class T>
void Write(const yhash_set<T>& parameter, IYsonConsumer* consumer);

// yhash_map
template <class T>
void Write(const yhash_map<Stroka, T>& parameter, IYsonConsumer* consumer);

// TYsonProducer
void Write(TYsonProducer parameter, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
