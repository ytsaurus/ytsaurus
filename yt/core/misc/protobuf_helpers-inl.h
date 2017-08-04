#pragma once
#ifndef PROTOBUF_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include protobuf_helpers.h"
#endif

#include "assert.h"

namespace NYT {

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
            DeserializeFromProto(&result, TRef::FromString(data));
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
            DeserializeFromProto(&result.Get(), TRef::FromString(data));
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
    serializedArray->Reserve(serializedArray->size());
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
    const TRange<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedField<TSerialized>* serializedArray,
    const TRange<TOriginal>& originalArray)
{
    NDetail::ToProtoArrayImpl(serializedArray, originalArray);
}

template <class TSerialized, class TOriginal>
void ToProto(
    ::google::protobuf::RepeatedPtrField<TSerialized>* serializedArray,
    const yhash_set<TOriginal>& originalArray)
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
}

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(TRefCountedProto<TProto>&& other)
{
    TProto::Swap(&other);
}

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(const TProto& other)
{
    TProto::CopyFrom(other);
}

template <class TProto>
TRefCountedProto<TProto>::TRefCountedProto(TProto&& other)
{
    TProto::Swap(&other);
}

//! Gives the extra allocated size for protobuf types.
//! This function is used for ref counted tracking.
template <class TProto>
size_t SpaceUsed(const TRefCountedProto<TProto>* instance)
{
    return sizeof(TRefCountedProto<TProto>) + instance->TProto::SpaceUsed() - sizeof(TProto);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
