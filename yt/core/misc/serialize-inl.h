#pragma once
#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TInput, class T>
void ReadPodImpl(TInput& input, T& obj, bool safe)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    auto loadBytes = input.Load(&obj, sizeof(obj));
    if (safe) {
        if (loadBytes != sizeof(obj)) {
            THROW_ERROR_EXCEPTION("Byte size mismatch while reading a pod")
                << TErrorAttribute("bytes_loaded", loadBytes)
                << TErrorAttribute("bytes_expected", sizeof(obj));
        }
    } else {
        YT_VERIFY(loadBytes == sizeof(obj));
    }
}

template <class TInput, class T>
void ReadPod(TInput& input, T& obj)
{
    ReadPodImpl(input, obj, false);
}

template <class TInput, class T>
void ReadPodOrThrow(TInput& input, T& obj)
{
    ReadPodImpl(input, obj, true);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void UnpackRefsImpl(const TSharedRef& packedRef, T* parts, bool safe)
{
    TMemoryInput input(packedRef.Begin(), packedRef.Size());

    i32 size;
    ReadPodImpl(input, size, safe);
    if (safe) {
        if (size < 0) {
            THROW_ERROR_EXCEPTION("Packed ref size is negative")
                << TErrorAttribute("size", size);
        }
    } else {
        YT_VERIFY(size >= 0);
    }

    parts->clear();
    parts->reserve(size);

    for (int index = 0; index < size; ++index) {
        i64 partSize;
        ReadPodImpl(input, partSize, safe);
        if (safe) {
            if (partSize < 0) {
                THROW_ERROR_EXCEPTION("A part of a packed ref has negative size")
                    << TErrorAttribute("index", index)
                    << TErrorAttribute("size", partSize);
            }
            if (packedRef.End() - input.Buf() < partSize) {
                THROW_ERROR_EXCEPTION("A part of a packed ref is too large")
                    << TErrorAttribute("index", index)
                    << TErrorAttribute("size", partSize)
                    << TErrorAttribute("bytes_left", packedRef.End() - input.Buf());
            }
        } else {
            YT_VERIFY(partSize >= 0);
            YT_VERIFY(packedRef.End() - input.Buf() >= partSize);
        }

        parts->push_back(packedRef.Slice(input.Buf(), input.Buf() + partSize));

        input.Skip(partSize);
    }

    if (safe) {
        if (input.Buf() < packedRef.End()) {
            THROW_ERROR_EXCEPTION("Packed ref is too large")
                << TErrorAttribute("extra_bytes", packedRef.End() - input.Buf());
        }
    } else {
        YT_VERIFY(input.Buf() == packedRef.End());
    }
}

template <class T>
void UnpackRefs(const TSharedRef& packedRef, T* parts)
{
    UnpackRefsImpl(packedRef, parts, false);
}

template <class T>
void UnpackRefsOrThrow(const TSharedRef& packedRef, T* parts)
{
    UnpackRefsImpl(packedRef, parts, true);
}

////////////////////////////////////////////////////////////////////////////////

inline constexpr TEntitySerializationKey::TEntitySerializationKey()
    : Index(-1)
{ }

inline constexpr TEntitySerializationKey::TEntitySerializationKey(int index)
    : Index(index)
{ }

inline constexpr bool TEntitySerializationKey::operator == (TEntitySerializationKey rhs)
{
    return Index == rhs.Index;
}

inline constexpr bool TEntitySerializationKey::operator != (TEntitySerializationKey rhs)
{
    return !(*this == rhs);
}

inline constexpr TEntitySerializationKey::operator bool() const
{
    return Index != -1;
}

inline void TEntitySerializationKey::Save(TEntityStreamSaveContext& context) const
{
    NYT::Save(context, Index);
}

inline void TEntitySerializationKey::Load(TEntityStreamLoadContext& context)
{
    NYT::Load(context, Index);
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TEntityStreamSaveContext::GenerateSerializationKey()
{
    return TEntitySerializationKey(SerializationKeyIndex_++);
}

////////////////////////////////////////////////////////////////////////////////

inline TEntitySerializationKey TEntityStreamLoadContext::RegisterEntity(void* entity)
{
    auto key = TEntitySerializationKey(static_cast<int>(Entities_.size()));
    Entities_.push_back(entity);
    return key;
}

template <class T>
T* TEntityStreamLoadContext::GetEntity(TEntitySerializationKey key) const
{
    YT_ASSERT(key.Index >= 0 && key.Index < static_cast<int>(Entities_.size()));
    return static_cast<T*>(Entities_[key.Index]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

