#ifndef COLLECTION_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include collection_helpers.h"
#endif
#undef COLLECTION_HELPERS_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ShrinkHashTable(T* collection)
{
    if (collection->bucket_count() > 4 * collection->size() && collection->bucket_count() > 16) {
        typename std::remove_reference<decltype(*collection)>::type collectionCopy(collection->begin(), collection->end());
        collectionCopy.swap(*collection);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

