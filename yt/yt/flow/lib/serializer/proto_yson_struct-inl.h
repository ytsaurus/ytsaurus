#ifndef PROTO_YSON_STRUCT_INL_H_
#error "Direct inclusion of this file is not allowed, include proto_yson_struct.h"
// For the sake of sane code completion.
#include "proto_yson_struct.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <CIsProtobufMessage TMessage>
TProtoYsonStruct<TMessage>::TProtoYsonStruct(const TMessage& message)
    : Message_(message)
{ }

template <CIsProtobufMessage TMessage>
TProtoYsonStruct<TMessage>::TProtoYsonStruct(TMessage&& message)
    : Message_(std::move(message))
{ }

template <CIsProtobufMessage TMessage>
TMessage& TProtoYsonStruct<TMessage>::operator*()
{
    return Message_;
}

template <CIsProtobufMessage TMessage>
const TMessage& TProtoYsonStruct<TMessage>::operator*() const
{
    return Message_;
}

template <CIsProtobufMessage TMessage>
TMessage* TProtoYsonStruct<TMessage>::operator->()
{
    return &Message_;
}

template <CIsProtobufMessage TMessage>
const TMessage* TProtoYsonStruct<TMessage>::operator->() const
{
    return &Message_;
}

template <CIsProtobufMessage TMessage>
template <NYTree::CYsonStructSource TSource>
void TProtoYsonStruct<TMessage>::Load(
    TSource source,
    bool /*postprocess*/,
    bool /*setDefaults*/,
    const NYPath::TYPath& /*path*/,
    std::optional<NYTree::EUnrecognizedStrategy> /*recursiveUnrecognizedStrategy*/)
{
    TString proto;
    Deserialize(proto, NYTree::NPrivate::TYsonSourceTraits<TSource>::AsNode(source));
    Message_.ParseFromStringOrThrow(proto);
}

template <CIsProtobufMessage TMessage>
void TProtoYsonStruct<TMessage>::Save(NYson::IYsonConsumer* consumer) const
{
    consumer->OnStringScalar(Message_.SerializeAsStringOrThrow());
}

template <CIsProtobufMessage TMessage>
void TProtoYsonStruct<TMessage>::WriteSchema(NYson::IYsonConsumer* consumer) const
{
    NYTree::NPrivate::WriteSchema(TStringBuf(), consumer);
}

////////////////////////////////////////////////////////////////////////////////

template <CIsProtobufMessage TMessage>
void Serialize(const TProtoYsonStruct<TMessage>& value, NYson::IYsonConsumer* consumer)
{
    value.Save(consumer);
}

template <CIsProtobufMessage TMessage, NYTree::CYsonStructSource TSource>
void Deserialize(TProtoYsonStruct<TMessage>& value, TSource source)
{
    value.Load(std::move(source));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
