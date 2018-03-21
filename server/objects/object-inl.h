#pragma once
#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
#endif

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TObject::ValidateAs() const
{
    auto expectedType = T::Type;
    auto actualType = GetType();
    if (expectedType != actualType) {
        THROW_ERROR_EXCEPTION("Invalid type of object %Qv: expected %Qlv, actual %Qlv",
            GetId(),
            expectedType,
            actualType);
    }
}

template <>
inline void TObject::ValidateAs<TObject>() const
{ }

template <class T>
const T* TObject::As() const
{
    ValidateAs<T>();
    return static_cast<const T*>(this);
}

template <class T>
T* TObject::As()
{
    ValidateAs<T>();
    return static_cast<T*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
