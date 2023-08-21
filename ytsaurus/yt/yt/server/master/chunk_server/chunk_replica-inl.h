#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
// For the sake of sane code completion.
#include "chunk_replica.h"
#endif

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/misc/serialize.h>

template <class T, bool WithReplicaState, int IndexCount, template <typename> class TAccessor>
struct THash<NYT::NChunkServer::TAugmentedPtr<T, WithReplicaState, IndexCount, TAccessor>>
{
    Y_FORCE_INLINE size_t operator()(NYT::NChunkServer::TAugmentedPtr<T, WithReplicaState, IndexCount, TAccessor> value) const
    {
        return value.GetHash();
    }
};

template <class T>
struct THash<NYT::NChunkServer::TCompatPtrWithIndexes<T>>
{
    Y_FORCE_INLINE size_t operator()(NYT::NChunkServer::TCompatPtrWithIndexes<T> value) const
    {
        return value.GetHash();
    }
};

namespace NYT::NChunkServer {

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

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr()
    : Value_(0)
{ }

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(T* ptr, int index)
    requires (!WithReplicaState && IndexCount == 1)
    : Value_(reinterpret_cast<uintptr_t>(ptr) | (static_cast<uintptr_t>(index) << 56))
{
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xff00000000000000LL) == 0);
    YT_ASSERT(index >= 0 && index <= 0xff);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(
    T* ptr,
    int index,
    EChunkReplicaState state)
    requires (WithReplicaState && IndexCount == 1)
    : Value_(
        reinterpret_cast<uintptr_t>(ptr) |
        static_cast<uintptr_t>(state) |
        (static_cast<uintptr_t>(index) << 56))
{
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xff00000000000003LL) == 0);
    YT_ASSERT(index >= 0 && index <= 0xff);
    YT_ASSERT(static_cast<uintptr_t>(state) <= 0x3);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(
    T* ptr,
    int firstIndex,
    int secondIndex)
    requires (!WithReplicaState && IndexCount == 2)
    : Value_(
        reinterpret_cast<uintptr_t>(ptr) |
        (static_cast<uintptr_t>(firstIndex) << 56) |
        (static_cast<uintptr_t>(secondIndex) << 48))
{
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xffff000000000000LL) == 0);
    YT_ASSERT(firstIndex >= 0 && firstIndex <= 0xff);
    YT_ASSERT(secondIndex >= 0 && secondIndex <= 0xff);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(
    T* ptr,
    int firstIndex,
    int secondIndex,
    EChunkReplicaState state)
    requires (WithReplicaState && IndexCount == 2)
    : Value_(
        reinterpret_cast<uintptr_t>(ptr) |
        static_cast<uintptr_t>(state) |
        (static_cast<uintptr_t>(firstIndex) << 56) |
        (static_cast<uintptr_t>(secondIndex) << 48))
{
    YT_ASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xffff000000000000LL) == 0);
    YT_ASSERT(firstIndex >= 0 && firstIndex <= 0xff);
    YT_ASSERT(secondIndex >= 0 && secondIndex <= 0xff);
    YT_ASSERT(static_cast<uintptr_t>(state) <= 0x3);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(
    TAugmentedPtr<T, true, IndexCount, TAugmentationAccessor> other)
    requires (!WithReplicaState)
    : Value_(other.ToGenericState().Value_)
{ }

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(
    TAugmentedPtr<T, false, IndexCount, TAugmentationAccessor> other)
    requires WithReplicaState
    : Value_(other.Value_)
{ }

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::TAugmentedPtr(
    TAugmentedPtr<T, false, IndexCount, TAugmentationAccessor> other,
    EChunkReplicaState state)
    requires WithReplicaState
    : Value_(other.Value_ | static_cast<uintptr_t>(state))
{ }

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE T* TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::GetPtr() const
{
    return reinterpret_cast<T*>(Value_ & 0x0000fffffffffffcLL);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
template <int Index>
Y_FORCE_INLINE int TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::GetIndex() const
    requires (Index <= IndexCount)
{
    return (Value_ >> (64 - 8 * Index)) & 0xff;
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE size_t TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::GetHash() const
{
    return static_cast<size_t>(Value_);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE EChunkReplicaState TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::GetReplicaState() const
    requires WithReplicaState
{
    return static_cast<EChunkReplicaState>(Value_ & 0x3);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE bool TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::operator==(TAugmentedPtr other) const
{
    return Value_ == other.Value_;
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE bool TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::operator!=(TAugmentedPtr other) const
{
    return Value_ != other.Value_;
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE bool TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::operator<(TAugmentedPtr other) const
{
    int thisFirstIndex = GetIndex<1>();
    int otherFirstIndex = other.GetIndex<1>();
    if (thisFirstIndex != otherFirstIndex) {
        return thisFirstIndex < otherFirstIndex;
    }

    if constexpr (WithReplicaState) {
        auto thisState = GetReplicaState();
        auto otherState = other.GetReplicaState();
        if (thisState != otherState) {
            return thisState < otherState;
        }
    }

    if constexpr (IndexCount == 2) {
        auto thisSecondIndex = GetIndex<2>();
        auto otherSecondIndex = other.GetIndex<2>();
        if (thisSecondIndex != otherSecondIndex) {
            return thisSecondIndex < otherSecondIndex;
        }
    }

    return GetPtr()->GetId() < other.GetPtr()->GetId();
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE bool TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::operator>(TAugmentedPtr other) const
{
    return other < *this;
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE bool TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::operator<=(TAugmentedPtr other) const
{
    return !operator>(other);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE bool TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::operator>=(TAugmentedPtr other) const
{
    return !operator<(other);
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
template <class C>
Y_FORCE_INLINE void TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::Save(C& context) const
{
    using NYT::Save;
    Save(context, GetPtr());
    Save<uint8_t>(context, GetIndex<1>());
    if constexpr (IndexCount == 2) {
        Save<uint8_t>(context, GetIndex<2>());
    }
    if constexpr (WithReplicaState) {
        Save(context, GetReplicaState());
    }
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
template <class C>
Y_FORCE_INLINE void TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::Load(C& context)
{
    using NYT::Load;
    auto* ptr = Load<T*>(context);
    int firstIndex = Load<uint8_t>(context);
    if constexpr (IndexCount == 2) {
        int secondIndex = Load<uint8_t>(context);
        if constexpr (WithReplicaState) {
            auto state = Load<EChunkReplicaState>(context);
            *this = TAugmentedPtr(ptr, firstIndex, secondIndex, state);
        } else {
            *this = TAugmentedPtr(ptr, firstIndex, secondIndex);
        }
    } else if constexpr (WithReplicaState) {
        auto state = Load<EChunkReplicaState>(context);
        *this = TAugmentedPtr(ptr, firstIndex, state);
    } else {
        *this = TAugmentedPtr(ptr, firstIndex);
    }
}

template <class T, bool WithReplicaState, int IndexCount, template <class> class TAugmentationAccessor>
Y_FORCE_INLINE TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor> TAugmentedPtr<T, WithReplicaState, IndexCount, TAugmentationAccessor>::ToGenericState() const
    requires WithReplicaState
{
    if constexpr (IndexCount == 1) {
        return TAugmentedPtr(GetPtr(), GetIndex<1>());
    } else {
        return TAugmentedPtr(GetPtr(), GetIndex<1>(), GetIndex<2>());
    }
}

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
Y_FORCE_INLINE int TAugmentedPtrReplicaIndexAccessor<TImpl>::GetReplicaIndex() const
{
    return static_cast<const TImpl*>(this)->template GetIndex<1>();
}

template <class TImpl>
Y_FORCE_INLINE int TAugmentedPtrMediumIndexAccessor<TImpl>::GetMediumIndex() const
{
    return static_cast<const TImpl*>(this)->template GetIndex<1>();
}

template <class TImpl>
Y_FORCE_INLINE int TAugmentedPtrReplicaAndMediumIndexAccessor<TImpl>::GetReplicaIndex() const
{
    return static_cast<const TImpl*>(this)->template GetIndex<1>();
}

template <class TImpl>
Y_FORCE_INLINE int TAugmentedPtrReplicaAndMediumIndexAccessor<TImpl>::GetMediumIndex() const
{
    return static_cast<const TImpl*>(this)->template GetIndex<2>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class T>
Y_FORCE_INLINE TCompatPtrWithIndexes<T>::TCompatPtrWithIndexes()
    : Value_(0)
{ }

template <class T>
Y_FORCE_INLINE TCompatPtrWithIndexes<T>::operator bool() const
{
    return Value_ != 0;
}

template <class T>
Y_FORCE_INLINE TCompatPtrWithIndexes<T>::TCompatPtrWithIndexes(T* ptr, int replicaIndex, int mediumIndex, EChunkReplicaState state)
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
Y_FORCE_INLINE T* TCompatPtrWithIndexes<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value_ & 0x000ffffffffffffcLL);
}

template <class T>
Y_FORCE_INLINE int TCompatPtrWithIndexes<T>::GetReplicaIndex() const
{
    return Value_ >> 52 & 0x1f;
}

template <class T>
Y_FORCE_INLINE int TCompatPtrWithIndexes<T>::GetMediumIndex() const
{
    return Value_ >> 57;
}

template <class T>
Y_FORCE_INLINE EChunkReplicaState TCompatPtrWithIndexes<T>::GetState() const
{
    return static_cast<EChunkReplicaState>(Value_ & 0x3);
}

template <class T>
Y_FORCE_INLINE TCompatPtrWithIndexes<T> TCompatPtrWithIndexes<T>::ToGenericState() const
{
    return TCompatPtrWithIndexes<T>(GetPtr(), GetReplicaIndex(), GetMediumIndex(), EChunkReplicaState::Generic);
}

template <class T>
Y_FORCE_INLINE size_t TCompatPtrWithIndexes<T>::GetHash() const
{
    return static_cast<size_t>(Value_);
}

template <class T>
Y_FORCE_INLINE bool TCompatPtrWithIndexes<T>::operator == (TCompatPtrWithIndexes other) const
{
    return Value_ == other.Value_;
}

template <class T>
Y_FORCE_INLINE bool TCompatPtrWithIndexes<T>::operator != (TCompatPtrWithIndexes other) const
{
    return Value_ != other.Value_;
}

template <class T>
Y_FORCE_INLINE bool TCompatPtrWithIndexes<T>::operator < (TCompatPtrWithIndexes other) const
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
Y_FORCE_INLINE bool TCompatPtrWithIndexes<T>::operator <= (TCompatPtrWithIndexes other) const
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
Y_FORCE_INLINE bool TCompatPtrWithIndexes<T>::operator > (TCompatPtrWithIndexes other) const
{
    return other < *this;
}

template <class T>
Y_FORCE_INLINE bool TCompatPtrWithIndexes<T>::operator >= (TCompatPtrWithIndexes other) const
{
    return other <= *this;
}

template <class T>
template <class C>
Y_FORCE_INLINE void TCompatPtrWithIndexes<T>::Save(C& context) const
{
    using NYT::Save;
    Save(context, GetPtr());
    Save<i8>(context, GetReplicaIndex());
    Save<i8>(context, GetMediumIndex());
    Save(context, GetState());
}

template <class T>
template <class C>
Y_FORCE_INLINE void TCompatPtrWithIndexes<T>::Load(C& context)
{
    using NYT::Load;
    auto* ptr = Load<T*>(context);
    int replicaIndex = Load<i8>(context);
    int mediumIndex = Load<i8>(context);
    auto state = Load<EChunkReplicaState>(context);
    *this = TCompatPtrWithIndexes<T>(ptr, replicaIndex, mediumIndex, state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
void TSerializerTraits<NChunkServer::TPtrWithReplicaInfo<T>, C>::TSerializer::Save(
    C& context,
    const NChunkServer::TPtrWithReplicaInfo<T>& replica)
{
    using NYT::Save;
    Save(context, replica.GetPtr());
    Save(context, replica.GetReplicaIndex());
    Save(context, replica.GetReplicaState());
}

template <class T, class C>
void TSerializerTraits<NChunkServer::TPtrWithReplicaInfo<T>, C>::TSerializer::Load(
    C& context,
    NChunkServer::TPtrWithReplicaInfo<T>& replica)
{
    using NYT::Load;
    using namespace NChunkServer;
    auto* ptr = Load<T*>(context);
    auto replicaIndex = Load<int>(context);
    auto replicaState = Load<EChunkReplicaState>(context);
    replica = TPtrWithReplicaInfo<T>(ptr, replicaIndex, replicaState);
}

template <class T, class C>
bool TSerializerTraits<NChunkServer::TPtrWithReplicaInfo<T>, C>::TComparer::Compare(
    const NChunkServer::TPtrWithReplicaInfo<T>& lhs,
    const NChunkServer::TPtrWithReplicaInfo<T>& rhs)
{
    using TPtrComparer = typename TSerializerTraits<T*, C>::TComparer;
    if (TPtrComparer::Compare(lhs.GetPtr(), rhs.GetPtr())) {
        return true;
    }
    if (TPtrComparer::Compare(rhs.GetPtr(), lhs.GetPtr())) {
        return false;
    }

    if (auto cmp = lhs.GetReplicaIndex() <=> rhs.GetReplicaIndex(); cmp != 0) {
        return cmp < 0;
    }

    return lhs.GetReplicaState() < rhs.GetReplicaState();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
