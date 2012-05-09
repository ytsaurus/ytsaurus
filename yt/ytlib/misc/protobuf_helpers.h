#pragma once

#include "guid.h"
#include "ref.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/extensions.pb.h>
#include <ytlib/misc/guid.pb.h>

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TProtoTraits
{
    static const T& ToProto(const T& value)
    {
        return value;
    }

    static const T& FromProto(const T& value)
    {
        return value;
    }
};

// TODO: generify for other classes providing their own ToProto/FromProto methods
template <>
struct TProtoTraits<TGuid>
{
    static NProto::TGuid ToProto(const TGuid& value)
    {
        return value.ToProto();
    }

    static TGuid FromProto(const NProto::TGuid& value)
    {
        return TGuid::FromProto(value);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TArrayItem, class TProtoItem>
inline void ToProto(
    ::google::protobuf::RepeatedPtrField<TProtoItem>* proto,
    const std::vector<TArrayItem>& array,
    bool clear = true)
{
    if (clear) {
        proto->Clear();
    }
    for (auto it = array.begin(); it != array.end(); ++it) {
        *proto->Add() = TProtoTraits<TArrayItem>::ToProto(*it);
    }
}

template <class T>
inline void ToProto(
    ::google::protobuf::RepeatedField<T>* proto,
    const std::vector<T>& array,
    bool clear = true)
{
    if (clear) {
        proto->Clear();
    }
    for (auto it = array.begin(); it != array.end(); ++it) {
        *proto->Add() = *it;
    }
}

template <class TArrayItem, class TProtoItem>
inline yvector<TArrayItem> FromProto(
    const ::google::protobuf::RepeatedPtrField<TProtoItem>& proto)
{
    yvector<TArrayItem> array(proto.size());
    for (int i = 0; i < proto.size(); ++i) {
        array[i] = TProtoTraits<TArrayItem>::FromProto(proto.Get(i));
    }
    return array;
}

template <class TArrayItem, class TProtoItem>
inline yvector<TArrayItem> FromProto(
    const ::google::protobuf::RepeatedField<TProtoItem>& proto)
{
    yvector<TArrayItem> array(proto.size());
    for (int i = 0; i < proto.size(); ++i) {
        array[i] = proto.Get(i);
    }
    return array;
}

////////////////////////////////////////////////////////////////////////////////

//! Serializes a given protobuf message into a given blob.
//! Return true iff everything was OK.
bool SerializeToProto(const google::protobuf::Message* message, TBlob* data);

//! Deserializes a given chunk of memory into a given protobuf message.
//! Return true iff everything was OK.
bool DeserializeFromProto(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

//! Serializes a given protobuf message into a given stream
//! Throw yexception() in case of error
void SaveProto(TOutputStream* output, const ::google::protobuf::Message& message);

//! Reads from a given stream protobuf message
//! Throw yexception() in case of error
void LoadProto(TInputStream* input, ::google::protobuf::Message& message);

////////////////////////////////////////////////////////////////////////////////

/*
 *  YT Extension Set is a collection of |(tag, data)| pairs.
 *  
 *  Here |tag| is a unique integer identifier and |data| is a protobuf-serialized
 *  embedded message.
 *  
 *  In contrast to native Protobuf Extensions, ours are deserialized on-demand.
 */

//! Returns an integer tag for a given type.
/*!
 *  Specialized versions of this function are generated with |DECLARE_PROTO_EXTENSION|.
 */
template <class T>
i32 GetProtoExtensionTag();

#define DECLARE_PROTO_EXTENSION(type, tag) \
    template <> \
    inline i32 GetProtoExtensionTag<type>() \
    { \
        return tag; \
    }

//! Finds and deserializes an extension of the given type. Fails if no matching
//! extension is found.
template <class T>
TAutoPtr<T> GetProtoExtension(const NProto::TExtensionSet& extensions);

//! Finds and deserializes an extension of the given type. Returns NULL if no matching
//! extension is found.
template <class T>
TAutoPtr<T> FindProtoExtension(const NProto::TExtensionSet& extensions);

//! Serializes and stores an extension.
//! Fails if extension with the same tag already exists.
template <class T>
void SetProtoExtension(NProto::TExtensionSet* extensions, const T& value);

//! Serializes and stores an extension.
//! Overwrites any extension with the same tag (if exists).
template <class T>
void UpdateProtoExtension(NProto::TExtensionSet* extensions, const T& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROTOBUF_HELPERS_INL_H_
#include "protobuf_helpers-inl.h"
#undef PROTOBUF_HELPERS_INL_H_

