#pragma once

#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
// For the sake of sane code completion.
#include "chunk_replica.h"
#endif

template <class T>
struct THash<NYT::NChunkServer::TPtrWithIndexes<T>>
{
    Y_FORCE_INLINE size_t operator()(NYT::NChunkServer::TPtrWithIndexes<T> value) const
    {
        return value.GetHash();
    }
};

#include <yt/server/master/cell_master/serialize.h>

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
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xff00000000000000LL) == 0);
    YT_ASSERT(index >= 0 && index <= 0xff);
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

static_assert(
    NChunkClient::ChunkReplicaIndexBound <= (1LL << 5),
    "Replica index must fit into 5 bits.");
static_assert(
    NChunkClient::MediumIndexBound <= (1LL << 7),
    "Medium index must fit into 7 bits.");
static_assert(
    static_cast<int>(TEnumTraits<EChunkReplicaState>::GetMaxValue()) < (1LL << 3),
    "Chunk replica state must fit into 2 bits.");

template <class T>
Y_FORCE_INLINE TPtrWithIndexes<T>::TPtrWithIndexes()
    : Value_(0)
{ }

template <class T>
Y_FORCE_INLINE TPtrWithIndexes<T>::TPtrWithIndexes(T* ptr, int replicaIndex, int mediumIndex, EChunkReplicaState state)
    : Value_(
        reinterpret_cast<uintptr_t>(ptr) |
        static_cast<uintptr_t>(state) |
        (static_cast<uintptr_t>(replicaIndex) << 52) |
        (static_cast<uintptr_t>(mediumIndex) << 57))
{
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xfff0000000000003LL) == 0);
    YT_ASSERT(replicaIndex >= 0 && replicaIndex <= 0x1f);
    YT_ASSERT(mediumIndex >= 0 && mediumIndex <= 0x7f);
}

template <class T>
Y_FORCE_INLINE T* TPtrWithIndexes<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value_ & 0x000ffffffffffffcLL);
}

template <class T>
Y_FORCE_INLINE int TPtrWithIndexes<T>::GetReplicaIndex() const
{
    return Value_ >> 52 & 0x1f;
}

template <class T>
Y_FORCE_INLINE int TPtrWithIndexes<T>::GetMediumIndex() const
{
    return Value_ >> 57;
}

template <class T>
Y_FORCE_INLINE EChunkReplicaState TPtrWithIndexes<T>::GetState() const
{
    return static_cast<EChunkReplicaState>(Value_ & 0x3);
}

template <class T>
Y_FORCE_INLINE TPtrWithIndexes<T> TPtrWithIndexes<T>::ToGenericState() const
{
    return TPtrWithIndexes<T>(GetPtr(), GetReplicaIndex(), GetMediumIndex(), EChunkReplicaState::Generic);
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

    auto thisState = GetState();
    auto otherState = other.GetState();
    if (thisState != otherState) {
        return thisState < otherState;
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

    auto thisState = GetState();
    auto otherState = other.GetState();
    if (thisState != otherState) {
        return thisState < otherState;
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
    Save(context, GetState());
}

template <class T>
template <class C>
Y_FORCE_INLINE void TPtrWithIndexes<T>::Load(C& context)
{
    using NYT::Load;
    auto* ptr = Load<T*>(context);
    int replicaIndex = Load<i8>(context);
    int mediumIndex = Load<i8>(context);
    EChunkReplicaState state;
    // COMPAT(babenko)
    if (context.GetVersion() < NCellMaster::EMasterReign::ErasureJournals) {
        state = EChunkReplicaState::Generic;
    } else {
        state = Load<EChunkReplicaState>(context);
    }
    *this = TPtrWithIndexes<T>(ptr, replicaIndex, mediumIndex, state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

