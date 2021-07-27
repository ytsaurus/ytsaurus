#pragma once

#include "guid.h"
#include "mpl.h"
#include "optional.h"
#include "object_pool.h"
#include "range.h"
#include "ref.h"
#include "serialize.h"
#include "small_vector.h"

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/defines.h>

#include <yt/yt_proto/yt/core/misc/proto/guid.pb.h>
#include <yt/yt_proto/yt/core/misc/proto/protobuf_helpers.pb.h>

#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline void ToProto(::google::protobuf::int64* serialized, TDuration original);
inline void FromProto(TDuration* original, ::google::protobuf::int64 serialized);

////////////////////////////////////////////////////////////////////////////////

inline void ToProto(::google::protobuf::int64* serialized, TInstant original);
inline void FromProto(TInstant* original, ::google::protobuf::int64 serialized);

////////////////////////////////////////////////////////////////////////////////

inline void ToProto(::google::protobuf::uint64* serialized, TInstant original);
inline void FromProto(TInstant* original, ::google::protobuf::uint64 serialized);

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename std::enable_if<std::is_convertible_v<T*, ::google::protobuf::MessageLite*>, void>::type ToProto(
    T* serialized,
    const T& original);
template <class T>
typename std::enable_if<std::is_convertible_v<T*, ::google::protobuf::MessageLite*>, void>::type FromProto(
    T* original,
    const T& serialized);

template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum && !TEnumTraits<T>::IsBitEnum, void>::type ToProto(
    int* serialized,
    T original);
template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum && !TEnumTraits<T>::IsBitEnum, void>::type FromProto(
    T* original,
    int serialized);

template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum && TEnumTraits<T>::IsBitEnum, void>::type ToProto(
    ui64* serialized,
    T original);
template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum && TEnumTraits<T>::IsBitEnum, void>::type FromProto(
    T* original,
    ui64 serialized);

////////////////////////////////////////////////////////////////////////////////

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const std::vector<TOriginal>& originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const std::vector<TOriginal>& originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const SmallVectorImpl<TOriginal>& originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const SmallVectorImpl<TOriginal>& originalArray);

template <class TSerialized, class TOriginal, size_t Size>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const TCompactVector<TOriginal, Size>& originalArray);

template <class TSerialized, class TOriginal, size_t Size>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const TCompactVector<TOriginal, Size>& originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const THashSet<TOriginal>& originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const THashSet<TOriginal>& originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    TRange<TOriginal> originalArray);

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    TRange<TOriginal> originalArray);

template <class TOriginalArray, class TSerialized>
void FromProto(
    TOriginalArray* originalArray,
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedArray);

template <class TOriginalArray, class TSerialized>
void FromProto(
    TOriginalArray* originalArray,
    const ::google::protobuf::RepeatedField<TSerialized>& serializedArray);

template <class TOriginal, class TSerialized>
void CheckedHashSetFromProto(
    THashSet<TOriginal>* originalHashSet,
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedHashSet);

template <class TOriginal, class TSerialized>
void CheckedHashSetFromProto(
    THashSet<TOriginal>* originalHashSet,
    const ::google::protobuf::RepeatedField<TSerialized>& serializedHashSet);

template <class TSerialized, class T, class E, E Min, E Max>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const TEnumIndexedVector<E, T, Min, Max>& originalArray);

template <class TSerialized, class T, class E, E Min, E Max>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const TEnumIndexedVector<E, T, Min, Max>& originalArray);

////////////////////////////////////////////////////////////////////////////////

template <class TSerialized, class TOriginal, class... TArgs>
TSerialized ToProto(const TOriginal& original, TArgs&&... args);

template <class TOriginal, class TSerialized, class... TArgs>
TOriginal FromProto(const TSerialized& serialized, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TEnvelopeFixedHeader
{
    ui32 EnvelopeSize;
    ui32 MessageSize;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

//! Serializes a protobuf message.
//! Fails on error.
TSharedRef SerializeProtoToRef(
    const google::protobuf::MessageLite& message,
    bool partial = true);

//! \see SerializeProtoToString
TString SerializeProtoToString(
    const google::protobuf::MessageLite& message,
    bool partial = true);

//! Deserializes a chunk of memory into a protobuf message.
//! Returns |true| iff everything went well.
bool TryDeserializeProto(
    google::protobuf::MessageLite* message,
    TRef data);

//! Deserializes a chunk of memory into a protobuf message.
//! Fails on error.
void DeserializeProto(
    google::protobuf::MessageLite* message,
    TRef data);

//! Serializes a given protobuf message and wraps it with envelope.
//! Optionally compresses the serialized message.
//! Fails on error.
TSharedRef SerializeProtoToRefWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId = NCompression::ECodec::None,
    bool partial = true);

//! \see SerializeProtoToRefWithEnvelope
TString SerializeProtoToStringWithEnvelope(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId = NCompression::ECodec::None,
    bool partial = true);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithEnvelope
//! and deserializes it into a protobuf message.
//! Returns |true| iff everything went well.
bool TryDeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    TRef data);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithEnvelope
//! and deserializes it into a protobuf message.
//! Fails on error.
void DeserializeProtoWithEnvelope(
    google::protobuf::MessageLite* message,
    TRef data);

//! Serializes a given protobuf message.
//! Optionally compresses the serialized message.
//! Fails on error.
TSharedRef SerializeProtoToRefWithCompression(
    const google::protobuf::MessageLite& message,
    NCompression::ECodec codecId = NCompression::ECodec::None,
    bool partial = true);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithCompression,
//! decompresses it with a given codec and deserializes it into a protobuf message.
//! Returns |true| iff everything went well.
bool TryDeserializeProtoWithCompression(
    google::protobuf::MessageLite* message,
    TRef data,
    NCompression::ECodec codecId = NCompression::ECodec::None);

//! Unwraps a chunk of memory obtained from #SerializeProtoToRefWithCompression,
//! decompresses it with a given codec and deserializes it into a protobuf message.
//! Fails on error.
void DeserializeProtoWithCompression(
    google::protobuf::MessageLite* message,
    TRef data,
    NCompression::ECodec codecId = NCompression::ECodec::None);

TSharedRef PushEnvelope(const TSharedRef& data);
TSharedRef PopEnvelope(const TSharedRef& data);

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
    typename std::enable_if_t<std::is_convertible_v<T&, ::google::protobuf::Message&>>>
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
    { \
        static constexpr i32 Value = tag; \
    };

//! Registers protobuf extension for further conversions. This method
//! is assumed to be called during static initialization only.
void RegisterProtobufExtension(
    const google::protobuf::Descriptor* descriptor,
    int tag,
    const TString& name);

#define REGISTER_PROTO_EXTENSION(type, tag, name) \
    const bool UNIQUE_NAME(TmpBool) = [] { \
        const auto* descriptor = type::default_instance().GetDescriptor(); \
        RegisterProtobufExtension(descriptor, tag, #name); \
        return false; \
    } ();

//! Finds and deserializes an extension of the given type. Fails if no matching
//! extension is found.
template <class T>
T GetProtoExtension(const NYT::NProto::TExtensionSet& extensions);

// Returns |true| iff an extension of a given type is present.
template <class T>
bool HasProtoExtension(const NYT::NProto::TExtensionSet& extensions);

//! Finds and deserializes an extension of the given type. Returns null if no matching
//! extension is found.
template <class T>
std::optional<T> FindProtoExtension(const NYT::NProto::TExtensionSet& extensions);

//! Serializes and stores an extension.
//! Overwrites any extension with the same tag (if exists).
template <class T>
void SetProtoExtension(NProto::TExtensionSet* extensions, const T& value);

//! Tries to remove the extension.
//! Returns |true| iff the proper extension is found.
template <class T>
bool RemoveProtoExtension(NProto::TExtensionSet* extensions);

//! Filters extensions leaving only those matching #tags set.
void FilterProtoExtensions(
    NYT::NProto::TExtensionSet* target,
    const NYT::NProto::TExtensionSet& source,
    const THashSet<int>& tags);
void FilterProtoExtensions(
    NYT::NProto::TExtensionSet* inplace,
    const THashSet<int>& tags);
NYT::NProto::TExtensionSet FilterProtoExtensions(
    const NYT::NProto::TExtensionSet& source,
    const THashSet<int>& tags);

////////////////////////////////////////////////////////////////////////////////

//! Wrapper that makes proto message ref-counted.
template <class TProto>
class TRefCountedProto
    : public TRefCounted
    , public TProto
{
public:
    TRefCountedProto() = default;
    TRefCountedProto(const TRefCountedProto<TProto>& other);
    TRefCountedProto(TRefCountedProto<TProto>&& other);
    explicit TRefCountedProto(const TProto& other);
    explicit TRefCountedProto(TProto&& other);
    ~TRefCountedProto();

    i64 GetSize() const;

private:
    size_t ExtraSpace_ = 0;

    void RegisterExtraSpace();
    void UnregisterExtraSpace();
};

////////////////////////////////////////////////////////////////////////////////

google::protobuf::Timestamp GetProtoNow();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define PROTOBUF_HELPERS_INL_H_
#include "protobuf_helpers-inl.h"
#undef PROTOBUF_HELPERS_INL_H_
