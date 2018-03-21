#pragma once
#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
#endif

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

template <class T>
const T* TObject::As() const
{
    return static_cast<const T*>(this);
}

template <class T>
T* TObject::As()
{
    return static_cast<T*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
