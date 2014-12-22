#ifndef CHUNK_REPLICA_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_replica.h"
#endif
#undef CHUNK_REPLICA_INL_H_

#include <core/misc/serialize.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////


template <class T>
TPtrWithIndex<T>::TPtrWithIndex()
    : Value_(0)
{ }

template <class T>
TPtrWithIndex<T>::TPtrWithIndex(T* ptr, int index)
    : Value_(reinterpret_cast<uintptr_t>(ptr) | (static_cast<uintptr_t>(index) << 56))
{
    YASSERT((reinterpret_cast<uintptr_t>(ptr) & 0xff00000000000000LL) == 0);
    YASSERT(index >= 0 && index <= 0xff);
}

template <class T>
T* TPtrWithIndex<T>::GetPtr() const
{
    return reinterpret_cast<T*>(Value_ & 0x00ffffffffffffffLL);
}

template <class T>
int TPtrWithIndex<T>::GetIndex() const
{
    return Value_ >> 56;
}

template <class T>
size_t TPtrWithIndex<T>::GetHash() const
{
    return static_cast<size_t>(Value_);
}

template <class T>
bool TPtrWithIndex<T>::operator == (TPtrWithIndex other) const
{
    return Value_ == other.Value_;
}

template <class T>
bool TPtrWithIndex<T>::operator != (TPtrWithIndex other) const
{
    return Value_ != other.Value_;
}

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
template <class C>
void TPtrWithIndex<T>::Save(C& context) const
{
    using NYT::Save;
    Save(context, GetPtr());
    Save<i8>(context, GetIndex());
}

template <class T>
template <class C>
void TPtrWithIndex<T>::Load(C& context)
{
    using NYT::Load;
    auto* ptr = Load<T*>(context);
    int index;
    // COMPAT(babenko)
    if (context.GetVersion() < 109) {
        index = Load<int>(context);
    } else{
        index = Load<i8>(context);
    }
    *this = TPtrWithIndex<T>(ptr, index);
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

