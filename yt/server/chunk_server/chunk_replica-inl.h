#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
#endif
#undef CHUNK_REPLICA_INL_H_

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

#ifdef __x86_64__

template <class T>
TWithIndex::TWithIndex()
    : Value(0)
{ }

template <class T>
TWithIndex<T>::TWithIndex(T* ptr, int index)
    : Value(reinterpret_cast<ui64>(ptr) | (static_cast<ui64>(index) << 60))
{
    YASSERT((reinterpret_cast<ui64>(ptr) & 0xf000000000000000LL) == 0);
    YASSERT(index >= 0 && index < 16);
}

template <class T>
T* TWithIndex<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value & 0x0fffffffffffffffLL);
}

template <class T>
int TWithIndex<T>::GetIndex() const
{
    return Value >> 60;
}

template <class T>
size_t TWithIndex<T>::GetHash() const
{
    return static_cast<size_t>(Value);
}

template <class T>
bool TWithIndex<T>::operator == (TWithIndex other) const
{
    return Value == other.Value;
}

template <class T>
bool TWithIndex<T>::operator != (TWithIndex other) const
{
    return Value != other.Value;
}

#else

template <class T>
TWithIndex<T>::TWithIndex()
    : Ptr(nullptr)
    , Index(0)
{ }

template <class T>
TWithIndex<T>::TWithIndex(TDataNode* ptr, int index)
    : Ptr(ptr)
    , Index(index)
{ }

template <class T>
T* TWithIndex<T>::GetPtr() const
{
    return Ptr;
}

template <class T>
int TWithIndex<T>::GetIndex() const
{
    return Index;
}

template <class T>
size_t TWithIndex<T>::GetHash() const
{
    return THash<TDataNode*>()(Ptr) * 497 +
           THash<int>()(Index);
}

template <class T>
bool TWithIndex<T>::operator == (TWithIndex other) const
{
    return Ptr == other.Ptr && Index == other.Index;
}

template <class T>
bool TWithIndex<T>::operator != (TWithIndex other) const
{
    return Ptr != other.Ptr || Index != other.Index;
}

#endif

template <class T>
bool TWithIndex<T>::operator < (TWithIndex other) const
{
    auto thisId = GetPtr()->GetId();
    auto otherId = other.GetPtr()->GetId();
    if (thisId != otherId) {
        return thisId < otherId;
    }
    return GetIndex() < other.GetIndex();
}

template <class T>
bool TWithIndex<T>::operator <= (TWithIndex other) const
{
    auto thisId = GetPtr()->GetId();
    auto otherId = other.GetPtr()->GetId();
    if (thisId != otherId) {
        return thisId < otherId;
    }
    return GetIndex() <= other.GetIndex();
}

template <class T>
bool TWithIndex<T>::operator > (TWithIndex other) const
{
    return other < *this;
}

template <class T>
bool TWithIndex<T>::operator >= (TWithIndex other) const
{
    return other <= *this;
}

template <class T>
void SaveObjectRef(const NCellMaster::TSaveContext& context, TWithIndex<T> value)
{
    NCellMaster::SaveObjectRef(context, value.GetPtr());
    NCellMaster::Save(context, value.GetIndex());
}

template <class T>
void LoadObjectRef(const NCellMaster::TLoadContext& context, TWithIndex<T>& value)
{
    T* ptr;
    LoadObjectRef(context, ptr);

    int index;
    // COMPAT(babenko)
    if (context.GetVersion() >= 8) {
        Load(context, index);
    } else {
        index = 0;
    }

    value = TWithIndex<T>(ptr, index);
}

template <class T>
bool CompareObjectsForSerialization(TWithIndex<T> lhs, TWithIndex<T> rhs)
{
    return lhs < rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT


//! A hasher for TWithIndex.
template <class T>
struct hash< NYT::NChunkServer::TWithIndex<T> >
{
    size_t operator()(NYT::NChunkServer::TWithIndex<T> value) const
    {
        return value.GetHash();
    }
};
