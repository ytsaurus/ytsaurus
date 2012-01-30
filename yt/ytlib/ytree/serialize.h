#pragma once

#include "ephemeral.h"
#include "yson_writer.h"

namespace NYT {

class TConfigurable;

namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CloneNode(
    const INode* node,
    INodeFactory* factory = GetEphemeralNodeFactory());

TYsonProducer::TPtr ProducerFromYson(TInputStream* input);

TYsonProducer::TPtr ProducerFromYson(const TYson& data);

TYsonProducer::TPtr ProducerFromNode(const INode* node);

INode::TPtr DeserializeFromYson(
    TInputStream* input,
    INodeFactory* factory = GetEphemeralNodeFactory());

INode::TPtr DeserializeFromYson(
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
T ParseYson(const TYson& yson);

////////////////////////////////////////////////////////////////////////////////

// TConfigurable::TPtr
template <class T>
void Read(
    TIntrusivePtr<T>& parameter,
    const NYTree::INode* node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T*, TConfigurable* >, int>::TType = 0);

// i64
void Read(i64& parameter, const NYTree::INode* node);

// i32
void Read(i32& parameter, const NYTree::INode* node);

// ui32
void Read(ui32& parameter, const NYTree::INode* node);

// ui16
void Read(ui16& parameter, const NYTree::INode* node);

// double
void Read(double& parameter, const NYTree::INode* node);

// Stroka
void Read(Stroka& parameter, const NYTree::INode* node);

// bool
void Read(bool& parameter, const NYTree::INode* node);

// TDuration
void Read(TDuration& parameter, const NYTree::INode* node);

// TGuid
void Read(TGuid& parameter, const NYTree::INode* node);

// TEnumBase
template <class T>
void Read(
    T& parameter,
    const NYTree::INode* node,
    typename NMpl::TEnableIf<NMpl::TIsConvertible< T, TEnumBase<T> >, int>::TType = 0);

// TNullable
template <class T>
void Read(TNullable<T>& parameter, const NYTree::INode* node);

// INode::TPtr
void Read(
    NYTree::INode::TPtr& parameter,
    const NYTree::INode* node);

// yvector
template <class T>
void Read(yvector<T>& parameter, const NYTree::INode* node);

// yhash_set
template <class T>
void Read(yhash_set<T>& parameter, const NYTree::INode* node);

// yhash_map
template <class T>
void Read(yhash_map<Stroka, T>& parameter, const NYTree::INode* node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
