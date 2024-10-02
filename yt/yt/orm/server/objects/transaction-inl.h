#ifndef TRANSACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "transaction.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* TTransaction::GetTypedObject(TObjectKey key, TObjectKey parentKey)
{
    auto* object = GetObject(T::Type, std::move(key), std::move(parentKey));
    YT_VERIFY(object);
    return object->template As<T>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
