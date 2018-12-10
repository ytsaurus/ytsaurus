#pragma once

#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
// For the sake of sane code completion.
#include "chunk_replica.h"
#endif

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/misc/serialize.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
Y_FORCE_INLINE TPtrWithIndex<T>::TPtrWithIndex()
    : Value_(0)
{ }

template <class T>
Y_FORCE_INLINE TPtrWithIndex<T>::TPtrWithIndex(T* ptr, int index)
    : Value_(reinterpret_cast<uintptr_t>(ptr) | (static_cast<uintptr_t>(index) << 56))
{
    Y_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xff00000000000000LL) == 0);
    Y_ASSERT(index >= 0 && index <= 0xff);
}

template <class T>
Y_FORCE_INLINE T* TPtrWithIndex<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value_ & 0x00ffffffffffffffLL);
}

template <class T>
Y_FORCE_INLINE int TPtrWithIndex<T>::GetIndex() const
{
    return Value_ >> 56;
}

template <class T>
Y_FORCE_INLINE size_t TPtrWithIndex<T>::GetHash() const
{
    return static_cast<size_t>(Value_);
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndex<T>::operator==(TPtrWithIndex other) const
{
    return Value_ == other.Value_;
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndex<T>::operator!=(TPtrWithIndex other) const
{
    return Value_ != other.Value_;
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndex<T>::operator<(TPtrWithIndex other) const
{
    int thisIndex = GetIndex();
    int otherIndex = other.GetIndex();
    if (thisIndex != otherIndex) {
        return thisIndex < otherIndex;
    }
    return GetPtr()->GetId() < other.GetPtr()->GetId();
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndex<T>::operator<=(TPtrWithIndex other) const
{
    int thisIndex = GetIndex();
    int otherIndex = other.GetIndex();
    if (thisIndex != otherIndex) {
        return thisIndex < otherIndex;
    }
    return GetPtr()->GetId() <= other.GetPtr()->GetId();
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndex<T>::operator>(TPtrWithIndex other) const
{
    return other < *this;
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndex<T>::operator>=(TPtrWithIndex other) const
{
    return other <= *this;
}

template <class T>
template <class C>
Y_FORCE_INLINE void TPtrWithIndex<T>::Save(C& context) const
{
    using NYT::Save;
    Save(context, GetPtr());
    Save<i8>(context, GetIndex());
}

template <class T>
template <class C>
Y_FORCE_INLINE void TPtrWithIndex<T>::Load(C& context)
{
    using NYT::Load;
    auto* ptr = Load<T*>(context);
    int index = Load<i8>(context);
    *this = TPtrWithIndex<T>(ptr, index);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
Y_FORCE_INLINE TPtrWithIndexes<T>::TPtrWithIndexes()
    : Value_(0)
{ }

template <class T>
Y_FORCE_INLINE TPtrWithIndexes<T>::TPtrWithIndexes(T* ptr, int replicaIndex, int mediumIndex)
    : Value_(
        reinterpret_cast<uintptr_t>(ptr) |
        (static_cast<uintptr_t>(replicaIndex) << 56) |
        (static_cast<uintptr_t>(mediumIndex) << 52))
{
    static_assert(
        NChunkClient::ChunkReplicaIndexBound * NChunkClient::MediumIndexBound <= 0x100,
        "Replica and medium indexes must fit into a single byte.");

    Y_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xfff0000000000000LL) == 0);
    Y_ASSERT(replicaIndex >= 0 && replicaIndex <= 0xff);
    Y_ASSERT(mediumIndex >= 0 && mediumIndex <= 0xf);
}

template <class T>
Y_FORCE_INLINE T* TPtrWithIndexes<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value_ & 0x000fffffffffffffLL);
}

template <class T>
Y_FORCE_INLINE int TPtrWithIndexes<T>::GetReplicaIndex() const
{
    return Value_ >> 56;
}

template <class T>
Y_FORCE_INLINE int TPtrWithIndexes<T>::GetMediumIndex() const
{
    return (Value_ >> 52) & 0xf;
}

template <class T>
Y_FORCE_INLINE size_t TPtrWithIndexes<T>::GetHash() const
{
    return static_cast<size_t>(Value_);
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndexes<T>::operator == (TPtrWithIndexes other) const
{
    return Value_ == other.Value_;
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndexes<T>::operator != (TPtrWithIndexes other) const
{
    return Value_ != other.Value_;
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndexes<T>::operator < (TPtrWithIndexes other) const
{
    int thisReplicaIndex = GetReplicaIndex();
    int otherReplicaIndex = other.GetReplicaIndex();
    if (thisReplicaIndex != otherReplicaIndex) {
        return thisReplicaIndex < otherReplicaIndex;
    }

    int thisMediumIndex = GetMediumIndex();
    int otherMediumIndex = other.GetMediumIndex();
    if (thisMediumIndex != otherMediumIndex) {
        return thisMediumIndex < otherMediumIndex;
    }

    return GetPtr()->GetId() < other.GetPtr()->GetId();
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndexes<T>::operator <= (TPtrWithIndexes other) const
{
    int thisReplicaIndex = GetReplicaIndex();
    int otherReplicaIndex = other.GetReplicaIndex();
    if (thisReplicaIndex != otherReplicaIndex) {
        return thisReplicaIndex < otherReplicaIndex;
    }

    int thisMediumIndex = GetMediumIndex();
    int otherMediumIndex = other.GetMediumIndex();
    if (thisMediumIndex != otherMediumIndex) {
        return thisMediumIndex < otherMediumIndex;
    }

    return GetPtr()->GetId() <= other.GetPtr()->GetId();
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndexes<T>::operator > (TPtrWithIndexes other) const
{
    return other < *this;
}

template <class T>
Y_FORCE_INLINE bool TPtrWithIndexes<T>::operator >= (TPtrWithIndexes other) const
{
    return other <= *this;
}

template <class T>
template <class C>
Y_FORCE_INLINE void TPtrWithIndexes<T>::Save(C& context) const
{
    using NYT::Save;
    Save(context, GetPtr());
    Save<i8>(context, GetReplicaIndex());
    Save<i8>(context, GetMediumIndex());
}

template <class T>
template <class C>
Y_FORCE_INLINE void TPtrWithIndexes<T>::Load(C& context)
{
    using NYT::Load;
    auto* ptr = Load<T*>(context);
    int replicaIndex = Load<i8>(context);
    int mediumIndex = DefaultStoreMediumIndex;
    // COMPAT(shakurov)
    if (context.GetVersion() >= 400) {
        mediumIndex = Load<i8>(context);
    }
    *this = TPtrWithIndexes<T>(ptr, replicaIndex, mediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

template <class T>
struct THash<NYT::NChunkServer::TPtrWithIndex<T>>
{
    Y_FORCE_INLINE size_t operator()(NYT::NChunkServer::TPtrWithIndex<T> value) const
    {
        return value.GetHash();
    }
};

template <class T>
struct THash<NYT::NChunkServer::TPtrWithIndexes<T>>
{
    Y_FORCE_INLINE size_t operator()(NYT::NChunkServer::TPtrWithIndexes<T> value) const
    {
        return value.GetHash();
    }
};
