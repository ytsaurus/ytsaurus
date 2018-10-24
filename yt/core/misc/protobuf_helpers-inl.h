#pragma once
#ifndef PROTOBUF_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include protobuf_helpers.h"
// For the sake of sane code completion
#include "protobuf_helpers.h"
#endif

#include "assert.h"

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

DEFINE_TRIVIAL_PROTO_CONVERSIONS(TString)
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

////////////////////////////////////////////////////////////////////////////////

inline void ToProto(::google::protobuf::int64* serialized, TDuration original)
{
    *serialized = original.MicroSeconds();
}

inline void FromProto(TDuration* original, ::google::protobuf::int64 serialized)
{
    *original = TDuration::MicroSeconds(serialized);
}

////////////////////////////////////////////////////////////////////////////////

inline void ToProto(::google::protobuf::int64* serialized, TInstant original)
{
    *serialized = original.MicroSeconds();
}

inline void FromProto(TInstant* original, ::google::protobuf::int64 serialized)
{
    *original = TInstant::MicroSeconds(serialized);
}

////////////////////////////////////////////////////////////////////////////////

inline void ToProto(::google::protobuf::uint64* serialized, TInstant original)
{
    *serialized = original.MicroSeconds();
}

inline void FromProto(TInstant* original, ::google::protobuf::uint64 serialized)
{
    *original = TInstant::MicroSeconds(serialized);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename std::enable_if<NMpl::TIsConvertible<T*, ::google::protobuf::MessageLite*>::Value, void>::type ToProto(
    T* serialized,
    const T& original)
{
    *serialized = original;
}

template <class T>
typename std::enable_if<NMpl::TIsConvertible<T*, ::google::protobuf::MessageLite*>::Value, void>::type FromProto(
    T* original,
    const T& serialized)
{
    *original = serialized;
}

template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type ToProto(
    int* serialized,
    T original)
{
    *serialized = static_cast<int>(original);
}

template <class T>
typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type FromProto(
    T* original,
    int serialized)
{
    *original = T(serialized);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GetProtoExtension(const NProto::TExtensionSet& extensions)
{
    // Intentionally complex to take benefit of RVO.
    T result;
    i32 tag = TProtoExtensionTag<T>::Value;
    bool found = false;
    for (const auto& extension : extensions.extensions()) {
        if (extension.tag() == tag) {
            const auto& data = extension.data();
            DeserializeProto(&result, TRef::FromString(data));
            found = true;
            break;
        }
    }
    YCHECK(found);
    return result;
}

template <class T>
bool HasProtoExtension(const NProto::TExtensionSet& extensions)
{
    i32 tag = TProtoExtensionTag<T>::Value;
    for (const auto& extension : extensions.extensions()) {
        if (extension.tag() == tag) {
            return true;
        }
    }
    return false;
}

template <class T>
TNullable<T> FindProtoExtension(const NProto::TExtensionSet& extensions)
{
    TNullable<T> result;
    i32 tag = TProtoExtensionTag<T>::Value;
    for (const auto& extension : extensions.extensions()) {
        if (extension.tag() == tag) {
            const auto& data = extension.data();
            result.Assign(T());
            DeserializeProto(&result.Get(), TRef::FromString(data));
            break;
        }
    }
    return result;
}

template <class T>
void SetProtoExtension(NProto::TExtensionSet* extensions, const T& value)
{
    i32 tag = TProtoExtensionTag<T>::Value;
    NYT::NProto::TExtension* extension = nullptr;
    for (auto& currentExtension : *extensions->mutable_extensions()) {
        if (currentExtension.tag() == tag) {
            extension = &currentExtension;
            break;
        }
    }
    if (!extension) {
        extension = extensions->add_extensions();
    }

    int size = value.ByteSize();
    TString str;
    str.resize(size);
    YCHECK(value.SerializeToArray(str.begin(), size));
    extension->set_data(str);
    extension->set_tag(tag);
}

template <class T>
bool RemoveProtoExtension(NProto::TExtensionSet* extensions)
{
    i32 tag = TProtoExtensionTag<T>::Value;
    for (int index = 0; index < extensions->extensions_size(); ++index) {
        const auto& currentExtension = extensions->extensions(index);
        if (currentExtension.tag() == tag) {
            // Make it the last one.
            extensions->mutable_extensions()->SwapElements(index, extensions->extensions_size() - 1);
            // And then drop.
            extensions->mutable_extensions()->RemoveLast();
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TSerializedArray, class TOriginalArray>
void ToProtoArrayImpl(
    TSerializedArray* serializedArray,
    const TOriginalArray& originalArray)
{
    serializedArray->Clear();
    serializedArray->Reserve(originalArray.size());
    for (const auto& item : originalArray) {
        ToProto(serializedArray->Add(), item);
    }
}

template <class TOriginalArray, class TSerializedArray>
void FromProtoArrayImpl(
    TOriginalArray* originalArray,
    const TSerializedArray& serializedArray)
{
    originalArray->clear();
    originalArray->resize(serializedArray.size());
    for (int i = 0; i < serializedArray.size(); ++i) {
        FromProto(&(*originalArray)[i], serializedArray.Get(i));
    }
}

template <class TProtoPair, class TValue>
typename std::enable_if<!std::is_trivial<TValue>::value, void>::type SetPairValueImpl(TProtoPair& pair, const TValue& value)
{
    ToProto(pair->mutable_value(), value);
}

template <class TProtoPair, class TValue>
typename std::enable_if<std::is_trivial<TValue>::value, void>::type SetPairValueImpl(TProtoPair& pair, const TValue& value)
{
   pair->set_value(value);
}

template <class TSerializedArray, class T, class E, E Min, E Max>
void ToProtoArrayImpl(
    TSerializedArray* serializedArray,
    const TEnumIndexedVector<T, E, Min, Max>& originalArray)
{
    serializedArray->Clear();
    for (auto key : TEnumTraits<E>::GetDomainValues()) {
        if (originalArray.IsDomainValue(key)) {
            const auto& value = originalArray[key];
            auto* pair = serializedArray->Add();
            pair->set_key(static_cast<i32>(key));
            SetPairValueImpl(pair, value);
        }
    }
}

template <class T, class E, E Min, E Max, class TSerializedArray>
void FromProtoArrayImpl(
    TEnumIndexedVector<T, E, Min, Max>* originalArray,
    const TSerializedArray& serializedArray)
{
    for (auto key : TEnumTraits<E>::GetDomainValues()) {
        if (originalArray->IsDomainValue(key)) {
            (*originalArray)[key] = T{};
        }
    }
    for (const auto& pair: serializedArray) {
        const auto& key = static_cast<E>(pair.key());
        if (originalArray->IsDomainValue(key)) {
            FromProto(&(*originalArray)[key], pair.value());
        }
    }
}

template <class TOriginal, class TSerializedArray>
void FromProtoArrayImpl(
    TMutableRange<TOriginal>* originalArray,
    const TSerializedArray& serializedArray)
{
    std::fill(originalArray->begin(), originalArray->end(), TOriginal());
    // NB: Only takes items with known indexes. Be careful when protocol is changed.
    for (int i = 0; i < serializedArray.size() && i < originalArray->Size(); ++i) {
        FromProto(&(*originalArray)[i], serializedArray.Get(i));
    }
}

} // namespace NDetail

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const std::vector<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const std::vector<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const SmallVectorImpl<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const SmallVectorImpl<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    TRange<TOriginal> originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    TRange<TOriginal> originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class T, class E, E Min, E Max>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const TEnumIndexedVector<T, E, Min, Max>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class T, class E, E Min, E Max>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const TEnumIndexedVector<T, E, Min, Max>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const THashSet<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TOriginalArray, class TSerialized>
void FromProto(
    TOriginalArray* originalArray,
    const ::google::protobuf::RepeatedPtrField<TSerialized>& serializedArray)
{
    NDetail::FromProtoArrayImpl(originalArray, serializedArray);
}

template <class TOriginalArray, class TSerialized>
void FromProto(
    TOriginalArray* originalArray,
    const ::google::protobuf::RepeatedField<TSerialized>& serializedArray)
{
    NDetail::FromProtoArrayImpl(originalArray, serializedArray);
}

////////////////////////////////////////////////////////////////////////////////

template <class TSerialized, class TOriginal, class... TArgs>
TSerialized ToProto(const TOriginal& original, TArgs&&... args)
{
    TSerialized serialized;
    ToProto(&serialized, original, std::forward<TArgs>(args)...);
    return serialized;
}

template <class TOriginal, class TSerialized, class... TArgs>
TOriginal FromProto(const TSerialized& serialized, TArgs&&... args)
{
    TOriginal original;
    FromProto(&original, serialized, std::forward<TArgs>(args)...);
    return original;
}

////////////////////////////////////////////////////////////////////////////////

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(const TRefCountedProto<TProto>& other)
{
    TProto::CopyFrom(other);
    RegisterExtraSpace();
}

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(TRefCountedProto<TProto>&& other)
{
    TProto::Swap(&other);
    RegisterExtraSpace();
}

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(const TProto& other)
{
    TProto::CopyFrom(other);
    RegisterExtraSpace();
}

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(TProto&& other)
{
    TProto::Swap(&other);
    RegisterExtraSpace();
}

template <class TProto>
TRefCountedProto<TProto>::~TRefCountedProto()
{
    UnregisterExtraSpace();
}

template <class TProto>
void TRefCountedProto<TProto>::RegisterExtraSpace()
{
    auto spaceUsed = TProto::SpaceUsed();
    Y_ASSERT(spaceUsed >= sizeof(TProto));
    Y_ASSERT(ExtraSpace_ == 0);
    ExtraSpace_ = TProto::SpaceUsed() - sizeof (TProto);
    auto cookie = GetRefCountedTypeCookie<TRefCountedProto<TProto>>();
    TRefCountedTrackerFacade::AllocateSpace(cookie, ExtraSpace_);
}

template <class TProto>
void TRefCountedProto<TProto>::UnregisterExtraSpace()
{
    if (ExtraSpace_ != 0) {
        auto cookie = GetRefCountedTypeCookie<TRefCountedProto<TProto>>();
        TRefCountedTrackerFacade::FreeSpace(cookie, ExtraSpace_);
    }
}

template <class TProto>
i64 TRefCountedProto<TProto>::GetSize() const
{
    return sizeof(this) + ExtraSpace_;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMovableProto<T>::TMovableProto(TMovableProto<T>&& other)
{
    Underlying_.Swap(&other.Underlying_);
}

template <class T>
TMovableProto<T>::TMovableProto(T&& other)
{
    Underlying_.Swap(&other);
}

template <class T>
TMovableProto<T>& TMovableProto<T>::operator=(TMovableProto<T>&& other)
{
    if (this != &other) {
        Underlying_.Swap(other.Underlying_);
    }
    return *this;
}

template <class T>
TMovableProto<T>& TMovableProto<T>::operator=(T&& other)
{
    Underlying_.Swap(&other);
    return *this;
}

template <class T>
T& TMovableProto<T>::Unwrap()
{
    return Underlying_;
}

template <class T>
const T& TMovableProto<T>::Unwrap() const
{
    return Underlying_;
}

template <class T>
TMovableProto<T>::operator T&()
{
    return Unwrap();
}

template <class T>
TMovableProto<T>::operator const T&() const
{
    return Unwrap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
