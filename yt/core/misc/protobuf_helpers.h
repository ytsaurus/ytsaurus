 #pragma once

#include "guid.h"
#include "ref.h"
#include "object_pool.h"
#include "small_vector.h"
#include "nullable.h"
#include "mpl.h"
#include "serialize.h"

#include <core/misc/protobuf_helpers.pb.h>
#include <core/misc/guid.pb.h>

#include <core/compression/public.h>

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_TRIVIAL_PROTO_CONVERSIONS(type)                   \
    inline void ToProto(type* serialized, type original)         \
    {                                                            \
        *serialized = original;                                  \
    }                                                            \
                                                                 \
    inline void FromProto(type* original, type serialized)       \
    {                                                            \
        *original = serialized;                                  \
    }

DEFINE_TRIVIAL_PROTO_CONVERSIONS(Stroka)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(i8)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(ui8)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(i16)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(ui16)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(i32)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(ui32)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(i64)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(ui64)
DEFINE_TRIVIAL_PROTO_CONVERSIONS(bool)

#undef DEFINE_TRIVIAL_PROTO_CONVERSIONS

template <class T>
inline void ToProto(
    T* serialized,
    const T& original,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, ::google::protobuf::MessageLite*>, int>::TType = 0)
{
    *serialized = original;
}

template <class T>
inline void FromProto(
    T* original,
    const T& serialized,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, ::google::protobuf::MessageLite*>, int>::TType = 0)
{
    *original = serialized;
}

template <class TSerialized, class TOriginal>
TSerialized ToProto(const TOriginal& original)
{
    TSerialized serialized;
    ToProto(&serialized, original);
    return serialized;
}

template <class TOriginal, class TSerialized>
TOriginal FromProto(const TSerialized& serialized)
{
    TOriginal original;
    FromProto(&original, serialized);
    return original;
}

////////////////////////////////////////////////////////////////////////////////

template <class TSerialized, class TOriginal>
inline void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const std::vector<TOriginal>& originalArray,
    bool clear = true)
{
    if (clear) {
        serializedArray->Clear();
    }
    for (auto it = originalArray.begin(); it != originalArray.end(); ++it) {
        ToProto(serializedArray->Add(), *it);
    }
}

template <class TSerialized, class TOriginal>
inline void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const std::vector<TOriginal>& originalArray,
    bool clear = true)
{
    if (clear) {
        serializedArray->Clear();
    }
    for (auto it = originalArray.begin(); it != originalArray.end(); ++it) {
        ToProto(serializedArray->Add(), *it);
    }
}

template <class TSerialized, class TOriginal, unsigned N>
inline void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const TSmallVector<TOriginal, N>& originalArray,
    bool clear = true)
{
    if (clear) {
        serializedArray->Clear();
    }
    for (auto it = originalArray.begin(); it != originalArray.end(); ++it) {
        ToProto(serializedArray->Add(), *it);
    }
}

template <class TOriginal, class TOriginalArray, class TSerialized>
inline TOriginalArray FromProto(
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedArray)
{
    TOriginalArray originalArray(serializedArray.size());
    for (int i = 0; i < serializedArray.size(); ++i) {
        FromProto(&originalArray[i], serializedArray.Get(i));
    }
    return originalArray;
}

template <class TOriginal, class TSerialized>
inline std::vector<TOriginal> FromProto(
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedArray)
{
    return FromProto<TOriginal, std::vector<TOriginal>, TSerialized>(serializedArray);
}

template <class TOriginal, class TOriginalArray, class TSerialized>
inline TOriginalArray FromProto(
    const ::google::protobuf::RepeatedField<TSerialized>& serializedArray)
{
    TOriginalArray originalArray(serializedArray.size());
    for (int i = 0; i < serializedArray.size(); ++i) {
        FromProto(&originalArray[i], serializedArray.Get(i));
    }
    return originalArray;
}

template <class TOriginal, class TSerialized>
inline std::vector<TOriginal> FromProto(
    const ::google::protobuf::RepeatedField<TSerialized>& serializedArray)
{
    return FromProto<TOriginal, std::vector<TOriginal>, TSerialized>(serializedArray);
}

////////////////////////////////////////////////////////////////////////////////

//! Serializes a protobuf message.
//! Returns |true| iff everything went well.
bool SerializeToProto(
    const google::protobuf::Message& message,
    TSharedRef* data);

//! Deserializes a chunk of memory into a protobuf message.
//! Returns |true| iff everything went well.
bool DeserializeFromProto(
    google::protobuf::Message* message,
    const TRef& data);

//! Serializes a given protobuf message and wraps it with envelope.
//! Optionally compresses the serialized message.
//! Returns |true| iff everything went well.
bool SerializeToProtoWithEnvelope(
    const google::protobuf::Message& message,
    TSharedRef* data,
    NCompression::ECodec codecId = NCompression::ECodec::None);

//! Unwraps a chunk of memory obtained from #SerializeToProtoWithEnvelope
//! and deserializes it into a protobuf message.
//! Returns |true| iff everything went well.
bool DeserializeFromProtoWithEnvelope(
    google::protobuf::Message* message,
    const TRef& data);

////////////////////////////////////////////////////////////////////////////////

struct TBinaryProtoSerializer
{
    //! Serializes a given protobuf message into a given stream.
    //! Throws an exception in case of error.
    static void Save(TStreamSaveContext& context, const ::google::protobuf::Message& message);

    //! Reads from a given stream protobuf message.
    //! Throws an exception in case of error.
    static void Load(TStreamLoadContext& context, ::google::protobuf::Message& message);
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T&, ::google::protobuf::Message&>>::TType>
{
    typedef TBinaryProtoSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

/*
 *  YT Extension Set is a collection of |(tag, data)| pairs.
 *
 *  Here |tag| is a unique integer identifier and |data| is a protobuf-serialized
 *  embedded message.
 *
 *  In contrast to native Protobuf Extensions, ours are deserialized on-demand.
 */

//! Used to obtain an integer tag for a given type.
/*!
 *  Specialized versions of this traits are generated with |DECLARE_PROTO_EXTENSION|.
 */
template <class T>
struct TProtoExtensionTag;

#define DECLARE_PROTO_EXTENSION(type, tag) \
    template <> \
    struct TProtoExtensionTag<type> \
        : NMpl::TIntegralConstant<i32, tag> \
    { };

//! Finds and deserializes an extension of the given type. Fails if no matching
//! extension is found.
template <class T>
T GetProtoExtension(const NProto::TExtensionSet& extensions);

//! Finds and deserializes an extension of the given type. Returns |Null| if no matching
//! extension is found.
template <class T>
TNullable<T> FindProtoExtension(const NProto::TExtensionSet& extensions);

//! Serializes and stores an extension.
//! Overwrites any extension with the same tag (if exists).
template <class T>
void SetProtoExtension(NProto::TExtensionSet* extensions, const T& value);

//! Tries to remove the extension.
//! Returns |true| iff the proper extension is found.
template <class T>
bool RemoveProtoExtension(NProto::TExtensionSet* extensions);

void FilterProtoExtensions(
    NProto::TExtensionSet* target,
    const NProto::TExtensionSet& source,
    const yhash_set<int>& tags);

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TPooledObjectTraits<
    T,
    typename NMpl::TEnableIf<NMpl::TIsConvertible<T*, ::google::protobuf::MessageLite*>, int>::TType>
    : public TPooledObjectTraitsBase
{
    static void Clean(::google::protobuf::MessageLite* obj)
    {
        obj->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROTOBUF_HELPERS_INL_H_
#include "protobuf_helpers-inl.h"
#undef PROTOBUF_HELPERS_INL_H_

