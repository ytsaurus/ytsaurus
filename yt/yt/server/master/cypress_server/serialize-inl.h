#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NCypressServer {

///////////////////////////////////////////////////////////////////////////////

template <class T>
TEntitySerializationKey TCellLocalObjectSaveRegistry<T>::RegisterObject(T* object)
{
    auto [it, inserted] = RegisteredObjects_.emplace(object, NextSerializationKey_);
    if (inserted) {
        ++NextSerializationKey_.Underlying();
    }
    return it->second;
}

template <class T>
const THashMap<T*, TEntitySerializationKey>& TCellLocalObjectSaveRegistry<T>::RegisteredObjects() const
{
    return RegisteredObjects_;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TCellLocalObjectLoadRegistry<T>::RegisterObject(TEntitySerializationKey key, T* object)
{
    YT_VERIFY(object);
    EmplaceOrCrash(RegisteredObjects_, key, object);
}

template <class T>
T* TCellLocalObjectLoadRegistry<T>::GetObjectOrThrow(TEntitySerializationKey key)
{
    auto it = RegisteredObjects_.find(key);

    if (it == RegisteredObjects_.end()) {
        THROW_ERROR_EXCEPTION("Unregistered serialization key %v", key);
    }

    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
