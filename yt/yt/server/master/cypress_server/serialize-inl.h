#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

namespace NYT::NCypressServer {

///////////////////////////////////////////////////////////////////////////////

template <class T>
TEntitySerializationKey TCellLocalObjectSaveRegistry<T>::RegisterObject(T* object)
{
    auto [it, inserted] = RegisteredObjects_.emplace(
        object,
        TEntitySerializationKey(NextSerializationKeyIndex_));

    if (inserted) {
        ++NextSerializationKeyIndex_;
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
    YT_VERIFY(RegisteredObjects_.emplace(key.Index, object).second);
}

template <class T>
T* TCellLocalObjectLoadRegistry<T>::GetObjectOrThrow(TEntitySerializationKey key)
{
    auto it = RegisteredObjects_.find(key.Index);

    if (it == RegisteredObjects_.end()) {
        THROW_ERROR_EXCEPTION("Unregistered serialization key %v", key.Index);
    }

    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
