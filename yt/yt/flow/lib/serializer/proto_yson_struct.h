#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CIsProtobufMessage = std::derived_from<std::decay_t<T>, google::protobuf::Message>;

////////////////////////////////////////////////////////////////////////////////

template <CIsProtobufMessage TMessage>
class TProtoYsonStruct
{
public:
    struct TImplementsYsonStructField;

    TProtoYsonStruct() = default;
    explicit TProtoYsonStruct(const TMessage& message);
    explicit TProtoYsonStruct(TMessage&& message);

    TMessage& operator*();
    const TMessage& operator*() const;

    TMessage* operator->();
    const TMessage* operator->() const;

    template <NYTree::CYsonStructSource TSource>
    void Load(
        TSource source,
        bool postprocess = true,
        bool setDefaults = true,
        const NYPath::TYPath& path = {},
        std::optional<NYTree::EUnrecognizedStrategy> recursiveUnrecognizedStrategy = {});

    void Save(NYson::IYsonConsumer* consumer) const;

    void WriteSchema(NYson::IYsonConsumer* consumer) const;

private:
    TMessage Message_;
};

////////////////////////////////////////////////////////////////////////////////

template <CIsProtobufMessage TMessage>
void Serialize(const TProtoYsonStruct<TMessage>& value, NYson::IYsonConsumer* consumer);

template <CIsProtobufMessage TMessage, NYTree::CYsonStructSource TSource>
void Deserialize(TProtoYsonStruct<TMessage>& value, TSource source);

} // namespace NYT::NFlow

#define PROTO_YSON_STRUCT_INL_H_
#include "proto_yson_struct-inl.h"
#undef PROTO_YSON_STRUCT_INL_H_
