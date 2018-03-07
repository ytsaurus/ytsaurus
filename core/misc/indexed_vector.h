#pragma once

#include "range.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple container consisting of a vector of elements with type |T|
//! plus a hashtable enabling fast mapping from elements to their indexes.
/*!
 *  All elements must be distinct.
 *
 *  Supports efficient linear scans and O(1)-time removal.
 *
 *  Feel free to extend whenever needed.
 */
template <class T>
class TIndexedVector
{
public:
    bool IsEmpty() const;
    size_t GetSize() const;

    void Clear();
    void Reserve(size_t size);

    void PushBack(const T& item);
    void Remove(const T& item);

    const T* Begin() const;
    const T* End() const;

    // STL interop.
    const T* begin() const;
    const T* end() const;

private:
    std::vector<T> Items_;
    THashMap<T, size_t> ItemToIndex_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INDEXED_VECTOR_INL_H_
#include "indexed_vector-inl.h"
#undef INDEXED_VECTOR_INL_H_
