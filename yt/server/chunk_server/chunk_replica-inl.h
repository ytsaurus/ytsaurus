#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
#endif
#undef CHUNK_REPLICA_INL_H_

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

#ifdef __x86_64__

template <class T>
TPtrWithIndex<T>::TPtrWithIndex()
    : Value(0)
{ }

template <class T>
TPtrWithIndex<T>::TPtrWithIndex(T* ptr, int index)
    : Value(reinterpret_cast<ui64>(ptr) | (static_cast<ui64>(index) << 56))
{
    YASSERT((reinterpret_cast<ui64>(ptr) & 0xff00000000000000LL) == 0);
    YASSERT(index >= 0 && index <= 0xff);
}

template <class T>
T* TPtrWithIndex<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value & 0x00ffffffffffffffLL);
}

template <class T>
int TPtrWithIndex<T>::GetIndex() const
{
    return Value >> 56;
}

template <class T>
size_t TPtrWithIndex<T>::GetHash() const
{
    return static_cast<size_t>(Value);
}

template <class T>
bool TPtrWithIndex<T>::operator == (TPtrWithIndex other) const
{
    return Value == other.Value;
}

template <class T>
bool TPtrWithIndex<T>::operator != (TPtrWithIndex other) const
{
    return Value != other.Value;
}

#else

template <class T>
TPtrWithIndex<T>::TPtrWithIndex()
    : Ptr(nullptr)
    , Index(0)
{ }

template <class T>
TPtrWithIndex<T>::TPtrWithIndex(T* ptr, int index)
    : Ptr(ptr)
    , Index(index)
{ }

template <class T>
T* TPtrWithIndex<T>::GetPtr() const
{
    return Ptr;
}

template <class T>
int TPtrWithIndex<T>::GetIndex() const
{
    return Index;
}

template <class T>
size_t TPtrWithIndex<T>::GetHash() const
{
    return THash<T*>()(Ptr) * 497 +
           THash<int>()(Index);
}

template <class T>
bool TPtrWithIndex<T>::operator == (TPtrWithIndex other) const
{
    return Ptr == other.Ptr && Index == other.Index;
}

template <class T>
bool TPtrWithIndex<T>::operator != (TPtrWithIndex other) const
{
    return Ptr != other.Ptr || Index != other.Index;
}

#endif

template <class T>
bool TPtrWithIndex<T>::operator < (TPtrWithIndex other) const
{
    auto thisId = GetPtr()->GetId();
    auto otherId = other.GetPtr()->GetId();
    if (thisId != otherId) {
        return thisId < otherId;
    }
    return GetIndex() < other.GetIndex();
}

template <class T>
bool TPtrWithIndex<T>::operator <= (TPtrWithIndex other) const
{
    auto thisId = GetPtr()->GetId();
    auto otherId = other.GetPtr()->GetId();
    if (thisId != otherId) {
        return thisId < otherId;
    }
    return GetIndex() <= other.GetIndex();
}

template <class T>
bool TPtrWithIndex<T>::operator > (TPtrWithIndex other) const
{
    return other < *this;
}

template <class T>
bool TPtrWithIndex<T>::operator >= (TPtrWithIndex other) const
{
    return other <= *this;
}

template <class T>
bool CompareObjectsForSerialization(TPtrWithIndex<T> lhs, TPtrWithIndex<T> rhs)
{
    return lhs < rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT


template <class T>
struct hash< NYT::NChunkServer::TPtrWithIndex<T> >
{
    size_t operator()(NYT::NChunkServer::TPtrWithIndex<T> value) const
    {
        return value.GetHash();
    }
};

