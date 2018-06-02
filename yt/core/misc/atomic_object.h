#pragma once

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A synchronization object to load and store nontrivial object.
//! It looks like atomics but for objects.
template <class T>
class TAtomicObject
{
public:
    TAtomicObject() = default;
    TAtomicObject(const T& other);

    template <class U>
    void Store(U&& u);

    T Load() const;

private:
    T Object_;
    NConcurrency::TReaderWriterSpinLock Spinlock_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOriginal, class TSerialized>
void ToProto(TSerialized* serialized, const TAtomicObject<TOriginal>& original);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATOMIC_OBJECT_INL_H_
#include "atomic_object-inl.h"
#undef ATOMIC_OBJECT_INL_H_
