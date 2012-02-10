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

TNodePtr CloneNode(
    const INode* node,
    INodeFactory* factory = GetEphemeralNodeFactory());

TYsonProducer::TPtr ProducerFromYson(TInputStream* input);

TYsonProducer::TPtr ProducerFromYson(const TYson& data);

TYsonProducer::TPtr ProducerFromNode(const INode* node);

TNodePtr DeserializeFromYson(
    TInputStream* input,
    INodeFactory* factory = GetEphemeralNodeFactory());

TNodePtr DeserializeFromYson(
    const TYson& yson,
    INodeFactory* factory = GetEphemeralNodeFactory());

TOutputStream& SerializeToYson(
    const INode* node,
    TOutputStream& output,
    EYsonFormat format = EYsonFormat::Binary);

TYson SerializeToYson(
    const INode* node,
    EYsonFormat format = EYsonFormat::Binary);

TYson SerializeToYson(
    TYsonProducer* producer,
    EYsonFormat format = EYsonFormat::Binary);

TYson SerializeToYson(
    const TConfigurable* config,
    EYsonFormat format = EYsonFormat::Binary);

template <class T>
TYson SerializeToYson(
    const T& value,
    EYsonFormat format = EYsonFormat::Binary);

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

template <class T>
typename TDeserializeTraits<T>::TReturnType DeserializeFromYson(const TYson& yson);

////////////////////////////////////////////////////////////////////////////////

// TConfigurable::TPtr
template <class T>
void Read(
    TIntrusivePtr<T>& parameter,
    const INode* node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TConfigurable*>, int>::TType = 0);

// i64
void Read(i64& parameter, const INode* node);

// i32
void Read(i32& parameter, const INode* node);

// ui32
void Read(ui32& parameter, const INode* node);

// ui16
void Read(ui16& parameter, const INode* node);

// double
void Read(double& parameter, const INode* node);

// Stroka
void Read(Stroka& parameter, const INode* node);

// bool
void Read(bool& parameter, const INode* node);

// TDuration
void Read(TDuration& parameter, const INode* node);

// TGuid
void Read(TGuid& parameter, const INode* node);

// TEnumBase
template <class T>
void Read(
    T& parameter,
    const INode* node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType = 0);

// TNullable
template <class T>
void Read(TNullable<T>& parameter, const INode* node);

// INode::TPtr
void Read(
    TNodePtr& parameter,
    const INode* node);

// yvector
template <class T>
void Read(yvector<T>& parameter, const INode* node);

// yhash_set
template <class T>
void Read(yhash_set<T>& parameter, const INode* node);

// yhash_map
template <class T>
void Read(yhash_map<Stroka, T>& parameter, const INode* node);

////////////////////////////////////////////////////////////////////////////////

// TConfigurable::TPtr
template <class T>
void Write(
    const TIntrusivePtr<T>& parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType = 0);

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

// TDuration
void Write(const TDuration& parameter, IYsonConsumer* consumer);

// TGuid
void Write(const TGuid& parameter, IYsonConsumer* consumer);

// TEnumBase
template <class T>
void Write(
    const T& parameter,
    IYsonConsumer* consumer,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, TEnumBase<T>*>, int>::TType = 0);

// TNullable
template <class T>
void Write(const TNullable<T>& parameter, IYsonConsumer* consumer);

// INode::TPtr
void Write(const TNodePtr& parameter, IYsonConsumer* consumer);

// yvector
template <class T>
void Write(const yvector<T>& parameter, IYsonConsumer* consumer);

// yhash_set
template <class T>
void Write(const yhash_set<T>& parameter, IYsonConsumer* consumer);

// yhash_map
template <class T>
void Write(const yhash_map<Stroka, T>& parameter, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
